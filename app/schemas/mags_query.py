from pydantic import BaseModel, Field, ValidationError, ConfigDict


class MagsQuery(BaseModel):
    accession: str = Field(min_length=3)
    catalogue: str = Field(min_length=3)
    model_config = ConfigDict(extra="forbid")