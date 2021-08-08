import inspect
from collections import OrderedDict
from typing import Callable, Iterator, Optional, Tuple, Union
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from requests.packages.urllib3.response import HTTPResponse
from requests.packages.urllib3.util import Url

from recipe_scrapers.settings import settings

from ._schemaorg import SchemaOrg

# some sites close their content for 'bots', so user-agent must be supplied
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:86.0) Gecko/20100101 Firefox/86.0"
}


class AbstractScraper:
    def __init__(
        self,
        url,
        proxies: Optional[str] = None,  # allows us to specify optional proxy server
        timeout: Optional[
            Union[float, Tuple, None]
        ] = None,  # allows us to specify optional timeout for request
        wild_mode: Optional[bool] = False,
    ):
        if settings.TEST_MODE:  # when testing, we load a file
            page_data = url.read()
            url = "https://test.example.com/"
        else:
            page_data = requests.get(
                url, headers=HEADERS, proxies=proxies, timeout=timeout
            ).content

        self.wild_mode = wild_mode
        self.soup = BeautifulSoup(page_data, "html.parser")
        self.url = url
        self.schema = SchemaOrg(page_data)

        # attach the plugins as instructed in settings.PLUGINS
        for name, func in inspect.getmembers(self, inspect.ismethod):
            current_method = getattr(self.__class__, name)
            for plugin in reversed(settings.PLUGINS):
                if plugin.should_run(self.host(), name):
                    current_method = plugin.run(current_method)
            setattr(self.__class__, name, current_method)

    @classmethod
    def host(cls) -> str:
        """get the host of the url, so we can use the correct scraper"""
        raise NotImplementedError("This should be implemented.")

    def canonical_url(self):
        canonical_link = self.soup.find("link", {"rel": "canonical", "href": True})
        if canonical_link:
            return urljoin(self.url, canonical_link["href"])
        return self.url

    def title(self):
        raise NotImplementedError("This should be implemented.")

    def total_time(self):
        """total time it takes to preparate the recipe in minutes"""
        raise NotImplementedError("This should be implemented.")

    def yields(self):
        """The number of servings or items in the recipe"""
        raise NotImplementedError("This should be implemented.")

    def image(self):
        raise NotImplementedError("This should be implemented.")

    def nutrients(self):
        raise NotImplementedError("This should be implemented.")

    def language(self):
        """
        Human language the recipe is written in.

        May be overridden by individual scrapers.
        """
        candidate_languages = OrderedDict()
        html = self.soup.find("html", {"lang": True})
        candidate_languages[html.get("lang")] = True

        # Deprecated: check for a meta http-equiv header
        # See: https://www.w3.org/International/questions/qa-http-and-lang
        meta_language = (
            self.soup.find(
                "meta",
                {
                    "http-equiv": lambda x: x and x.lower() == "content-language",
                    "content": True,
                },
            )
            if settings.META_HTTP_EQUIV
            else None
        )
        if meta_language:
            language = meta_language.get("content").split(",", 1)[0]
            if language:
                candidate_languages[language] = True

        # If other langs exist, remove 'en' commonly generated by HTML editors
        if len(candidate_languages) > 1:
            candidate_languages.pop("en", None)

        # Return the first candidate language
        return candidate_languages.popitem(last=False)[0]

    def ingredients(self):
        raise NotImplementedError("This should be implemented.")

    def instructions(self):
        raise NotImplementedError("This should be implemented.")

    def ratings(self):
        raise NotImplementedError("This should be implemented.")

    def author(self):
        raise NotImplementedError("This should be implemented.")

    def reviews(self):
        raise NotImplementedError("This should be implemented.")

    def links(self):
        invalid_href = {"#", ""}
        links_html = self.soup.findAll("a", href=True)

        return [link.attrs for link in links_html if link["href"] not in invalid_href]

    def site_name(self):
        meta = self.soup.find("meta", property="og:site_name")
        return meta.get("content") if meta else None

    URI_FORMAT = None

    @classmethod
    def does_recipe_exist(cls, uri: str, head_response: HTTPResponse = None):
        raise NotImplementedError("This should be implemented.")

    @classmethod
    def site_urls(
        cls,
        should_exclude_recipe: Callable[[Url, int], bool] = None,
        recipe_check_threads: int = 4,
    ) -> Iterator[Url]:

        raise NotImplementedError("This should be implemented.")
