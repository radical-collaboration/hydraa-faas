from pydantic import BaseModel, Field
from typing import Optional, Dict, Any

# request models for invoke and delete

class InvocationRequest(BaseModel):
    id: str
    payload: Optional[Dict[str, Any]] = Field(default_factory=dict)

class DeleteRequest(BaseModel):
    id: str