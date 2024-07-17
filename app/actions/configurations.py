from pydantic import Field

from .core import AuthActionConfiguration
from ..webhooks import JQTransformConfig


# Authentication
# token
# basic auth

# Cellstop / Digit animal / Mella tracking

class AuthenticateConfig(AuthActionConfiguration):
    auth_type: str
    auth_url: str
    username: str
    password: str
    token: str
    token_field: str
    auth_header: str


class GenericPullConfig(JQTransformConfig):
    url: str
    output_type: str = Field(..., description="Output type for the transformed data: 'obv' or 'event'")

