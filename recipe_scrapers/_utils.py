import html
import re
from collections import defaultdict
from threading import Lock

from ._exceptions import ElementNotFoundInHtml

TIME_REGEX = re.compile(
    r"(\D*(?P<hours>\d+)\s*(hours|hrs|hr|h|óra))?(\D*(?P<minutes>\d+)\s*(minutes|mins|min|m|perc))?",
    re.IGNORECASE,
)

SERVE_REGEX_NUMBER = re.compile(r"(\D*(?P<items>\d+)?\D*)")

SERVE_REGEX_ITEMS = re.compile(
    r"\bsandwiches\b |\btacquitos\b | \bmakes\b | \bcups\b | \bappetizer\b | \bporzioni\b",
    flags=re.I | re.X,
)

SERVE_REGEX_TO = re.compile(r"\d+(\s+to\s+|-)\d+", flags=re.I | re.X)


def get_minutes(element, return_zero_on_not_found=False):
    if element is None:
        # to be removed
        if return_zero_on_not_found:
            return 0
        raise ElementNotFoundInHtml(element)

    # handle integer in string literal
    try:
        return int(element)
    except Exception:
        pass

    if isinstance(element, str):
        time_text = element
    else:
        time_text = element.get_text()
    if time_text.startswith("P") and "T" in time_text:
        time_text = time_text.split("T", 2)[1]
    if "-" in time_text:
        time_text = time_text.split("-", 2)[
            1
        ]  # sometimes formats are like this: '12-15 minutes'
    if "h" in time_text:
        time_text = time_text.replace("h", "hours") + "minutes"

    matched = TIME_REGEX.search(time_text)

    minutes = int(matched.groupdict().get("minutes") or 0)
    minutes += 60 * int(matched.groupdict().get("hours") or 0)

    return minutes


def get_yields(element):
    """
    Will return a string of servings or items, if the receipt is for number of items and not servings
    the method will return the string "x item(s)" where x is the quantity.
    :param element: Should be BeautifulSoup.TAG, in some cases not feasible and will then be text.
    :return: The number of servings or items.
    """
    if element is None:
        raise ElementNotFoundInHtml(element)

    if isinstance(element, str):
        serve_text = element
    else:
        serve_text = element.get_text()

    if SERVE_REGEX_TO.search(serve_text):
        serve_text = serve_text.split(SERVE_REGEX_TO.split(serve_text, 2)[1], 2)[1]

    matched = SERVE_REGEX_NUMBER.search(serve_text).groupdict().get("items") or 0
    servings = "{} serving(s)".format(matched)

    if SERVE_REGEX_ITEMS.search(serve_text) is not None:
        # This assumes if object(s), like sandwiches, it is 1 person.
        # Issue: "Makes one 9-inch pie, (realsimple-testcase, gives "9 items")
        servings = "{} item(s)".format(matched)

    return servings


def normalize_string(string):
    # Convert all named and numeric character references (e.g. &gt;, &#62;)
    unescaped_string = html.unescape(string)
    return re.sub(
        r"\s+",
        " ",
        unescaped_string.replace("\xa0", " ")
        .replace("\n", " ")  # &nbsp;
        .replace("\t", " ")
        .strip(),
    )


def url_path_to_dict(path):
    pattern = (
        r"^"
        r"((?P<schema>.+?)://)?"
        r"((?P<user>.+?)(:(?P<password>.*?))?@)?"
        r"(?P<host>.*?)"
        r"(:(?P<port>\d+?))?"
        r"(?P<path>/.*?)?"
        r"(?P<query>[?].*?)?"
        r"$"
    )
    regex = re.compile(pattern)
    matches = regex.match(path)
    url_dict = matches.groupdict() if matches is not None else None

    return url_dict


def get_host_name(url):
    return url_path_to_dict(url.replace("://www.", "://"))["host"]


class StatusCodeLimiter:
    """
    While crawling a recipe website, how do you know when you've reached the
    end of a website's ID-based URL range?

    Some websites have consecutive ID-based URLs-soon as you hit a 404
    you're done. But, some have sparse numbering, so skipping a few doesn't
    mean you've passed the last recipe ID. It might take 40 or 100 or 200
    before you're really sure.

    So: feed this class each HTTP response status code from every URL your
    crawler attempts, and when it hits your preset limit of consecutive 404s
    (or whatever code you prefer), it raises StopIteration.

    It also counts all status codes received, to provide a simple frequency
    of all status codes (helpful when first crawling as site).

    Example:
        my_logger = logging.getLogger(__name__)
        code_limiter = StatusCodeLimiter(404, 50, my_logger)

        try:
            for recipe_id = range(10000, 15000):
                resp = requests.head(f"https://greatestrecipes.com/{recipe_id}")
                code_limiter.add(resp.status_code)
        except StopIteration:
            # done with crawl
            ...
    """

    def __init__(self, watch_status_code: int, max_consecutive_count: int, logger=None):
        """
        watch_status_code:int - a HTTP status code to monitor for a streak
        max_consecutive_count:int - the maximum of watch_stats_code allowed
                                  before raising StopIteration
        logger:logging.Logger - a logger instance to which the count of all
                              status codes will be written as a one-line
                              "report"
        """
        self._watch_status_code = watch_status_code
        self._max_consecutive_count = max_consecutive_count
        self._logger = logger
        self._thread_lock = Lock()

        # Remember the last code so we'll know if a streak was broken
        self.last_status_code = None

        # Counter for watch_status_code
        self.consecutive_code_count = 0

        # Count all codes, it has been helpful
        self.all_codes = defaultdict(int)

    def add(self, code: int):
        """
        code:int - an HTTP response code
        """
        try:
            # Allow only one thread to increment a var at a time
            self._thread_lock.acquire()

            # Is this the code we're watching for?
            if code == self._watch_status_code:
                if self.last_status_code == self._watch_status_code:
                    # Streak is continuing
                    self.consecutive_code_count += 1
                else:
                    # First instance of watch_status_code
                    self.consecutive_code_count = 1

                # If we received more than allowed consecutive
                # watch_status_code, signal to caller that the crawl is finished
                if self.consecutive_code_count >= self._max_consecutive_count:
                    # Log the count of all status codes if that was requested
                    if self._logger:
                        self._logger.info(
                            "End of crawl HTTP status code counts: "
                            + self.status_codes_report()
                        )
                    raise StopIteration(
                        f"Consecutive 404 count reached: {self._max_consecutive_count}"
                    )
            else:
                # Reset the counter if code != watch_status_code
                self.consecutive_code_count = 0

            self.last_status_code = code
            self.all_codes[code] += 1
        finally:
            self._thread_lock.release()

    def status_codes_report(self, delimiter: str = ", "):
        return delimiter.join(
            f"{code}={count}" for code, count in self.all_codes.items()
        )
