import inspect
import logging
import multiprocessing.dummy as mp
import re
import time
from typing import Callable, Iterator

import numpy as np
import requests

# from requests import Session
from requests.packages.urllib3.response import HTTPResponse
from requests.packages.urllib3.util import Url, parse_url

from ._abstract import AbstractScraper

logging.basicConfig()
logger = logging.getLogger(__name__)


UNITIZERX = re.compile(r"^([0-9.]+)\s*([^0-9.]*)$")
EXT_NUTRS_SEL = "section.recipe-nutrition.nutrition-section div.nutrition-row"
EXT_NUTRS_CLASH = "Extended nutrient name clashes with basic nutrient:" "%s = %s vs %s"
EXT_NUTR_PCT = "Got unit type from a percentage value: %s = %s (%s)"
DICT_FIELDS = [
    "author",
    "canonical_url",
    "image",
    "ingredients",
    "instructions",
    "language",
    "links",
    "ratings",
    "site_name",
    "title",
    "total_time",
    # "url",
    "yields",
]


class AllRecipes(AbstractScraper):
    URI_FORMAT = "https://www.allrecipes.com/recipe/%d"

    @classmethod
    def host(cls):
        return "allrecipes.com"

    def author(self):
        # NB: In the schema.org 'Recipe' type, the 'author' property is a
        # single-value type, not an ItemList.
        # allrecipes.com seems to render the author property as a list
        # containing a single item under some circumstances.
        # In those cases, the SchemaOrg class will fail due to the unexpected
        # type, and this method is called as a fallback.
        # Rather than implement non-standard handling in SchemaOrg, this code
        # provides a (hopefully temporary!) allrecipes-specific workaround.
        author = self.schema.data.get("author")
        if author and isinstance(author, list) and len(author) == 1:
            author = author[0].get("name")
        return author

    def title(self):
        return self.schema.title()

    def total_time(self):
        return self.schema.total_time()

    def yields(self):
        return self.schema.yields()

    def image(self):
        return self.schema.image()

    def ingredients(self):
        return self.schema.ingredients()

    def instructions(self):
        return self.schema.instructions()

    def ratings(self):
        try:
            return self.schema.ratings()
        except Exception:
            return None

    def nutrients(self):
        def unhandled(n):
            logger.warn("Unhandled AllRecipes extended nutrient format: %d", n)

        base_nutr = self.schema.nutrients()

        # Find extended nutrients
        ext_nutrs = {}
        try:
            ext_nutr_nodes = self.soup.select(EXT_NUTRS_SEL)
            for node in ext_nutr_nodes:
                tupl = list(node.stripped_strings)
                if len(tupl) >= 2:
                    name = tupl[0].strip(":")
                    ext_nutrs[name] = tupl[1]
                    if len(tupl) == 3:
                        if "%" in tupl[2]:
                            ext_nutrs[name + "%"] = tupl[2].strip(" %")
                        else:
                            unhandled(node)
                else:
                    unhandled(node)

        except BaseException as excep:
            logger.warning(
                "Unable to get extended nutrients from markup in "
                "AllRecipes.nutrients() - %s - %s",
                self.canonical_url(),
                str(type(excep)),
            )

        # Marry basic and extended nutrients, reporting name clashes
        for name, value in ext_nutrs.items():
            if name in base_nutr:
                logger.warn(EXT_NUTRS_CLASH, name, value, ext_nutrs[name])
                base_nutr[f"Ext {name}"] = value
            else:
                base_nutr[name] = value

        return base_nutr

    def nutrients_unitized(self):
        unitized = {}
        for name, value in self.nutrients().items():
            try:
                new_value = UNITIZERX.match(value).groups()
            except AttributeError:
                new_value = (value, None)
            if name.endswith("%"):
                if new_value[1]:
                    logger.warn(EXT_NUTR_PCT, name, value, str(new_value[1]))
                else:
                    new_value = (value, "RDA")

            # Special cases: transfer unit types found in name
            if not new_value[1]:
                if "calories" in name:
                    new_value = (value, "calories")
            unitized[name] = (float(new_value[0]), new_value[1])
        return unitized

    def to_dict(
        self, html=False, links=False, unitized=False, skip_attribs=None, uri=None
    ):
        NON_CONDITIONAL_FIELDS = {
            "author": self.schema.author,
            "canonical_url": self.canonical_url,
            "image": self.schema.image,
            "ingredients": self.schema.ingredients,
            "instructions": self.schema.instructions,
            "language": self.schema.language,
            "ratings": self.ratings,
            "site_name": self.site_name,
            "title": self.schema.title,
            "total_time": self.schema.total_time,
            "yields": self.schema.yields,
        }
        obj = {}
        for field, method in NON_CONDITIONAL_FIELDS.items():
            try:
                obj[field] = method()
            except BaseException as excep:
                logger.warning(
                    "Failed to get field %s on %s " "because: %s",
                    field,
                    uri,
                    str(excep)[:255],
                )
        if html:
            obj["html"] = str(self.soup)
        if links:
            obj["links"] = self.links()
        if unitized:
            obj["nutrients"] = self.nutrients_unitized()
        else:
            obj["nutrients"] = self.nutrients()
        return obj

    def _old_to_dict(self, html=False, unitized=False, skip_attribs=None, uri=None):
        obj = {}
        try:
            for attrib_name in DICT_FIELDS:
                if skip_attribs and attrib_name in skip_attribs:
                    continue
                # attrib = getattr(self.__class__, attrib_name, None)
                attrib = getattr(self, attrib_name, None)
                if attrib:
                    if inspect.isfunction(attrib) or inspect.ismethod(attrib):
                        # obj[attrib_name] = self[attrib]()
                        try:
                            obj[attrib_name] = attrib()
                        except BaseException as attribExcep:
                            obj[attrib_name] = attrib()
                            logger.warning(
                                "Couldn't get method attrib '%s'", attrib_name
                            )
                            logger.warning("Couldn't get method attrib '%s'", uri)
                            logger.warning(
                                "Couldn't get method attrib " "'%s'",
                                str(attribExcep)[:255],
                            )
                    else:
                        obj[attrib_name] = attrib
                else:
                    logger.warn("Expected attrib not found: %s", attrib_name)
            if html:
                obj["html"] = str(self.soup)
            if unitized:
                obj["nutrients"] = self.nutrients_unitized()
            else:
                obj["nutrients"] = self.nutrients()
        except Exception as serialExcep:
            logger.error(
                "Couldn't serialize recipe (%s): %s", uri, str(serialExcep)[:255]
            )
        finally:
            return obj

    @classmethod
    def does_recipe_exist(
        cls: AbstractScraper,
        uri: str,
        # requests_session: Session,
        head_response: HTTPResponse = None,
    ):
        if head_response is None:
            head_response = requests.head(uri)
        # For existing recipes, AllRecipes.com 301 redirects to complete uri
        # Otherwise it returns 404
        # print(head_response.status_code, head_response.url)
        return head_response.status_code == 301

    @classmethod
    def site_urls(
        cls,
        # requests_session: Session,
        should_exclude_recipe: Callable[[Url, int], bool] = None,
        recipe_check_threads: int = 4,
    ) -> Iterator[Url]:
        """
        This generator yields intrinsic or discoverable URLs for a site,
        which could be produced by a numeric range, or some other deterministic
        process but external process.
        """

        def recipe_id_to_permalink(recipe_id: int):
            """
            recipe_id: int - allrecipes.com's public-facing ID
            returns: urllib3.util.Url of existing recipes that can be GET
            """
            try:
                # Does caller want to exclude this recipe, for whatever reason?
                uri = cls.URI_FORMAT % recipe_id
                if should_exclude_recipe and should_exclude_recipe(uri):
                    logger.warn("Skipping %s", uri)
                    return None

                # Is this recipe fetchable?
                # logger.warning("HEAD %s", uri)
                head_resp = requests.head(uri)
                # head_resp = requests_session.head(uri, allow_redirects=True)
                if cls.does_recipe_exist(uri, head_resp):
                    redir_path = head_resp.headers.get("Location")
                    url = parse_url(uri)
                    permalink = Url(scheme=url.scheme, host=url.host, path=redir_path)
                    print(f"HEAD yes: {uri} to {permalink}")
                    return permalink
                # else:
                #     print(f"HEAD NO: {head_resp.status_code} - {uri}")
            except Exception as xxx:
                print(xxx)
            return None

        # AllRecipes.com's recipe IDs exist in a sparse matrix. We check HEAD
        # to see whether a given ID exists. If true, we yield the permalink.
        # If false, we yield None.
        # recipe_ids = np.arange(8800, 300000)
        # recipe_ids = np.arange(6662, 300000)
        recipe_ids = np.arange(6662, 10000)
        # recipe_ids = np.arange(6600, 6700)
        # np.random.shuffle(recipe_ids)
        with mp.Pool(recipe_check_threads) as p:
            # Run all recipe IDs X check_recipe()
            permalink_gen = p.imap_unordered(recipe_id_to_permalink, recipe_ids)

            try:
                while True:
                    permalink = next(permalink_gen)
                    # print("permalink ", permalink)
                    if permalink is not None:
                        yield permalink
                        time.sleep(0)
            except StopIteration:
                return
