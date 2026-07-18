class ApiError(Exception):
    """Raised anywhere in a request; rendered as {"error": {"code", "message"}}."""

    def __init__(self, status_code: int, code: str, message: str):
        self.status_code = status_code
        self.code = code
        self.message = message
        super().__init__(message)


def not_found(resource: str) -> ApiError:
    return ApiError(404, "not_found", f"{resource} not found")


def unauthorized(message: str = "invalid or missing credentials") -> ApiError:
    return ApiError(401, "unauthorized", message)
