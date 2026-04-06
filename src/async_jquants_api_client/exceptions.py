class JQuantsError(Exception):
    pass


class JQuantsAuthError(JQuantsError):
    pass


class JQuantsAPIError(JQuantsError):
    def __init__(self, status_code: int, message: str) -> None:
        self.status_code = status_code
        super().__init__(f"HTTP {status_code}: {message}")
