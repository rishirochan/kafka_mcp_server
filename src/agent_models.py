from typing import Annotated, List, Any, Optional

from pydantic import BaseModel, Field
from langgraph.graph.message import add_messages


class State(BaseModel):
    messages: Annotated[List[Any], add_messages]
    success_criteria: str
    feedback_on_work: Optional[str] = None
    success_criteria_met: bool = False
    user_input_needed: bool = False


class DataValidatorOutput(BaseModel):
    feedback: str = Field(
        description="Feedback on the data generator's response"
    )
    success_criteria_met: bool = Field(
        description="Whether the success criteria have been met"
    )
    user_input_needed: bool = Field(
        description="True if more input is needed from the user, or clarifications, "
        "or the data generator is stuck"
    )
