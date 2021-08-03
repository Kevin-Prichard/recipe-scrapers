import inspect
import logging
import multiprocessing.dummy as mp
import re
import typing as t

import numpy as np
import requests
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
    "url",
    "yields",
]


class AllRecipes(AbstractScraper):
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
        return self.schema.ratings()

    def nutrients(self):
        def unhandled(n):
            logger.warn(f"Unhandled AllRecipes extended nutrient format: {n}")

        # Find extended nutrients
        ext_nutrs = {}
        for node in self.soup.select(EXT_NUTRS_SEL):
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

        # Marry basic and extended nutrients, reporting name clashes
        nutr = self.schema.nutrients()
        for name, value in ext_nutrs.items():
            if name in nutr:
                logger.warn(EXT_NUTRS_CLASH, name, value, ext_nutrs[name])
                nutr[f"Ext {name}"] = value
            else:
                nutr[name] = value

        return nutr

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

    def to_dict(self, html=False, unitized=False, skip_attribs=None):
        obj = {}
        for attrib_name in DICT_FIELDS:
            if skip_attribs and attrib_name in skip_attribs:
                continue
            attrib = getattr(self.__class__, attrib_name, None)
            if attrib:
                if inspect.isfunction(attrib) or inspect.ismethod(attrib):
                    obj[attrib_name] = attrib(self)
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
        return obj

    @staticmethod
    def site_iterator(
        can_fetch: t.Callable[[t.AnyStr], bool] = None,
        lower_bound: int = 0,
        upper_bound: int = 100,
    ) -> t.Iterator[t.AnyStr]:
        uri_format = "https://www.allrecipes.com/recipe/%d"

        def recipe_finder() -> t.Iterator[bool]:
            def check_recipe(recipe_id):
                # print("check_recipe: %s" % str(recipe_id))
                try:
                    uri = uri_format % recipe_id
                    if can_fetch and can_fetch(recipe_id):
                        r = requests.head(uri)
                        if r.status_code == 301:
                            redir_path = r.headers.get("Location")
                            url = parse_url(uri)
                            new_uri = Url(
                                scheme=url.scheme, host=url.host, path=redir_path
                            )
                            print(f"HEAD yes: {uri} to {new_uri}")
                            # print("\n".join(f"    {k}: {v}"
                            #                 for k, v in r.headers.items()))
                            return str(new_uri)
                        else:
                            print(f"HEAD NO: {r.status_code} - {uri}")
                    # else:
                    #     print("URI in cache: ", uri)
                except Exception as xxx:
                    print(xxx)
                return False

            # Some borrowing: https://github.com/kaelynn-rose/RecipeEDA/pulse
            # rand_recipe_ids = np.arange(6663, 283432)
            rand_recipe_ids = np.arange(lower_bound, upper_bound)
            print("Randomizing...")
            # rand_recipe_ids = np.arange(6663, 7000)
            np.random.shuffle(rand_recipe_ids)
            with mp.Pool(4) as p:
                # Run all recipe IDs X check_recipe()
                yield from p.imap_unordered(check_recipe, rand_recipe_ids)
                # url_set = p.imap_unordered(check_recipe, rand_recipe_ids)
                # print("site_iterator.recipe_finder..url_set = "+str(url_set))

                """
                try:
                    while True:
                        # print("site_iterator.recipe_finder ...")
                        url = next(url_set)
                        # print("site_iterator.recipe_finder: url ... "+str(url))
                        yield url
                except StopIteration:
                    print("Done at site_iterator.recipe_finder StopIteration")
                    return
                """

        def recipe_fetcher() -> t.Iterator[t.AnyStr]:
            # Delegate results of URL creation and HEAD pulls
            yield from recipe_finder()
            """
            recipe_url_gen = recipe_finder()
            while True:
                # print("site_iterator.recipe_fetcher ...")
                try:
                    url = next(recipe_url_gen)
                    # print("site_iterator.recipe_fetcher: url = "+str(url))
                    yield url
                except StopIteration:
                    print("Done at site_iterator.recipe_fetcher StopIteration")
                    return
            """
            # with mp.Pool(1) as p:
            #     yield from p.imap_unordered(print, recipe_finder())
            # yield from p.imap_unordered(requests.get, recipe_finder())

        return recipe_fetcher()
