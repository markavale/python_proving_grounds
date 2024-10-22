import time
import functools
import logging
import colorlog
import requests
from typing import Callable, Type, Tuple, Any, Optional, Union, Iterable

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Create a color formatter
color_formatter = colorlog.ColoredFormatter(
    "%(log_color)s%(asctime)s - %(levelname)s - %(message)s",
    datefmt=None,
    reset=True,
    log_colors={
        'DEBUG': 'cyan',
        'INFO': 'green',
        'WARNING': 'yellow',
        'ERROR': 'red',
        'CRITICAL': 'bold_red',
    }
)

# Create a stream handler with the color formatter
handler = logging.StreamHandler()
handler.setFormatter(color_formatter)
logger.addHandler(handler)



def elapsed_time_logger(func):
    """Decorator to log the elapsed time of a function."""
    
    def wrapper(*args, **kwargs):
        start_time = time.time()
        logger.debug(f"Starting execution of {func.__name__}")
        
        # Execute the wrapped function
        result = func(*args, **kwargs)
        
        end_time = time.time()
        elapsed_time = end_time - start_time
        logger.debug(f"Finished execution of {func.__name__}")
        logger.info(f"Elapsed time for {func.__name__}: {elapsed_time:.10f}s")
        print(f"Elapsed time for {func.__name__}: {elapsed_time:.10f}s")
        
        return result
    
    return wrapper

class Retry:
    """
    A class that encapsulates retry logic for operations that might fail and need to be retried.
    """

    def __init__(
        self,
        max_retries: int = 3,
        initial_delay: float = 1.0,
        backoff_factor: float = 2.0,
        exceptions_to_check: Tuple[Type[BaseException], ...] = (Exception,),
        err_back: Optional[Union[
            Callable[[Exception, int, Tuple[Any, ...], dict], None],
            Iterable[Callable[[Exception, int, Tuple[Any, ...], dict], None]]
        ]] = None,
        on_failure: Optional[Union[
            Callable[[Exception, int, Tuple[Any, ...], dict], Any],
            Iterable[Callable[[Exception, int, Tuple[Any, ...], dict], Any]]
        ]] = None,
    ):
        """
        Initialize the Retry instance.

        Args:
            max_retries (int): Maximum number of retry attempts.
            initial_delay (float): Initial delay between retries in seconds.
            backoff_factor (float): Multiplicative factor to increase delay after each retry.
            exceptions_to_check (tuple): Exceptions that trigger a retry when caught.
            err_back (callable or iterable of callables, optional): Function(s) to call when an error or retry occurs.
                Each function should accept the exception instance, the retry attempt number, and the payload (args and kwargs) as arguments.
            on_failure (callable or iterable of callables, optional): Function(s) to call when all retries have been exhausted.
                Each function should accept the exception instance, the total number of attempts, and the payload (args and kwargs) as arguments.
        """
        self.max_retries = max_retries
        self.initial_delay = initial_delay
        self.backoff_factor = backoff_factor
        self.exceptions_to_check = exceptions_to_check

        # Normalize err_back into a list of callables
        if err_back is None:
            self.err_backs = []
        elif callable(err_back):
            self.err_backs = [err_back]
        else:
            self.err_backs = list(err_back)

        # Normalize on_failure into a list of callables
        if on_failure is None:
            self.on_failures = []
        elif callable(on_failure):
            self.on_failures = [on_failure]
        else:
            self.on_failures = list(on_failure)

    def __call__(self, func: Callable) -> Callable:
        """
        Make instances of Retry class callable. This allows the class to be used as a decorator.

        Args:
            func (callable): The function to wrap with retry logic.

        Returns:
            callable: The wrapped function.
        """

        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            """
            The actual wrapper function that implements the retry logic.

            Args:
                *args: Positional arguments to pass to the function.
                **kwargs: Keyword arguments to pass to the function.

            Returns:
                Any: The result of the function call.

            Raises:
                Exception: The last exception raised if all retries fail.
            """
            attempts = 0
            delay = self.initial_delay
            last_err_back_result = None  # To store the result from err_back
            last_exception = None  # To store the last exception

            while attempts < self.max_retries:
                try:
                    logger.debug(f"Attempt {attempts + 1} for function '{func.__name__}'.")
                    return func(*args, **kwargs)
                except self.exceptions_to_check as e:
                    last_exception = e
                    attempts += 1
                    logger.warning(
                        f"Exception occurred: {e}. Retrying {attempts}/{self.max_retries} after {delay} seconds..."
                    )
                    # Call each err_back function if any
                    for eb in self.err_backs:
                        try:
                            logger.debug(f"Calling err_back function '{eb.__name__}' due to exception.")
                            last_err_back_result = eb(e, attempts, args, kwargs)
                        except Exception as callback_exception:
                            logger.error(f"Exception in err_back function '{eb.__name__}': {callback_exception}")
                    time.sleep(delay)
                    delay *= self.backoff_factor

            # After retries are exhausted
            logger.error(f"All {self.max_retries} retries failed for function '{func.__name__}'.")
            # Call on_failure functions if any
            result_from_on_failure = None
            for failure_callback in self.on_failures:
                try:
                    logger.debug(f"Calling on_failure function '{failure_callback.__name__}' after retries exhausted.")
                    result = failure_callback(last_exception, attempts, args, kwargs)
                    if result is not None:
                        result_from_on_failure = result  # Store result
                except Exception as failure_callback_exception:
                    logger.error(f"Exception in on_failure function '{failure_callback.__name__}': {failure_callback_exception}")

            if result_from_on_failure is not None:
                return result_from_on_failure

            if last_err_back_result is not None:
                return last_err_back_result

            raise last_exception
        return wrapper
    
# Define multiple error callback functions
def error_callback_log(exception, attempt_number, args, kwargs):
    # Log the error
    logger.info(f"[LOG] Error: {exception} on attempt {attempt_number}")
    logger.info(f"[LOG] Function arguments were: args={args}, kwargs={kwargs}")

def error_callback_notify(exception, attempt_number, args, kwargs):
    # Notify an external service or system admin
    logger.error(f"[NOTIFY] Alert! Exception: {exception}")
    # Here you could add code to send an email or a message to a monitoring system

def error_callback_cleanup(exception, attempt_number, args, kwargs):
    # Perform cleanup actions
    print(f"args: {args}")
    print(f"kwargs: {kwargs}")
    print(f"kwargs: {dict(**kwargs)}")
    logger.error(f"[CLEANUP] Performing cleanup due to exception: {exception}")
    # Add any necessary cleanup logic

# Use the Retry class as a decorator with multiple err_back functions

@Retry(
    max_retries=3,
    initial_delay=1.0,
    backoff_factor=1.5,
    exceptions_to_check=(ValueError, Exception),
    err_back=[error_callback_log, error_callback_notify, error_callback_cleanup],
)
@elapsed_time_logger
def risky_operation(x, y):
    # Simulate an operation that may fail
    if x < 0:
        raise ValueError("Negative value encountered!")
    return x * y
@Retry(
    max_retries=3,
    initial_delay=1.0,
    backoff_factor=1.5,
    exceptions_to_check=(ValueError, Exception),
    err_back=[error_callback_log, error_callback_notify, error_callback_cleanup],
)
@elapsed_time_logger
def test_func():
    logger.info("start")
    time.sleep(2)
    logger.info("end")


def clean_up_url_callback(exception, attempt_number, args, kwargs):
    logger.error(f"[CLEANUP] Performing cleanup due to exception: {exception}")
    logger.info(f"Cleaning up url: {args[0]}")
    cleaned_url = args[0].replace("111", "").strip()
    logger.info(f"Cleaned up url: {cleaned_url}")
    response = requests.get(cleaned_url, timeout=5)
    response.raise_for_status()  # Raise an error for bad responses
    if response.text:
        with open("clean_up_url_callback.txt", "w") as f:
            f.write(response.text)
        return "return data from clean_up_url_callback"
    return None
@Retry(
    max_retries=3,
    initial_delay=1.0,
    backoff_factor=2.0,
    exceptions_to_check=(requests.exceptions.RequestException,),
    err_back=[error_callback_notify],
    on_failure=clean_up_url_callback
)
def fetch_website_data(url: str) -> str:
    """
    Fetches data from a specified website.

    Args:
        url (str): The URL of the website to fetch data from.

    Returns:
        str: The content of the website.

    Raises:
        requests.exceptions.RequestException: If the request fails after retries.
    """
    logger.info(f"Attempting to fetch data from {url}")
    response = requests.get(url, timeout=5)
    response.raise_for_status()  # Raise an error for bad responses
    if response.text:
        return "return data from fetch_website_data"
    return None
    # return response.text

def test_func_one():
    # Call the function
    try:
        result = risky_operation(-5, y=10)
    except Exception as e:
        logger.error(f"Operation failed after retries: {e}")

def test_func_two():
    # Call the function
    try:
        website_content = fetch_website_data("https://www.example111.com")
        print(website_content, "response")
        logger.info("Website data fetched successfully.")
    except Exception as e:
        logger.error(f"Failed to fetch website data after retries: {e}")

def default_function():
    logger.info("No function selected")

if __name__ == "__main__":
    import sys
    funcs: dict = {
        0: test_func,
        1: test_func_one,
        2: test_func_two,
    }
    input_args = int(sys.argv[1])
    final = funcs.get(input_args, default_function)
    res = final()
    logger.info(f"Result: {res}")