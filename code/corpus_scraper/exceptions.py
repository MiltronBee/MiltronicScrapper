"""
Custom exception classes for granular error handling throughout the framework.
"""


class ScrapingError(Exception):
    """Base exception for all scraping-related errors."""
    pass


class ConfigurationError(ScrapingError):
    """Raised when there are issues with configuration files or settings."""
    pass


class RobotsBlockedError(ScrapingError):
    """Raised when a URL is blocked by robots.txt rules."""
    pass


class ContentValidationError(ScrapingError):
    """Raised when extracted content fails validation checks."""
    pass


class LanguageMismatchError(ContentValidationError):
    """Raised when content is not in the required language."""
    pass


class ContentTooShortError(ContentValidationError):
    """Raised when content doesn't meet minimum length requirements."""
    pass


class ExtractionFailedError(ScrapingError):
    """Raised when all extraction methods fail to produce content."""
    pass


class StateManagementError(ScrapingError):
    """Raised when there are issues with state persistence or recovery."""
    pass


class NetworkError(ScrapingError):
    """Raised for network-related issues during scraping."""
    pass