from __future__ import annotations

from enum import Enum, verify, UNIQUE, auto
from typing import Optional


@verify(UNIQUE)
class ErrorCode(Enum):
    DivideByZero = auto()
    NaN = auto()
    Infinity = auto()
    InvalidOperand = auto()
    InvalidPendingOperator = auto()
    InvalidTableOperand = auto()
    ReferenceRequired = auto()
    SeeErrorMessage = auto()
    StackOverflow = auto()
    StackUnderflow = auto()
    OperandDataTypeMismatch = auto()
    OperandRequired = auto()
    UnimplementedStatistic = auto()
    UnimplementedTransformation = auto()
    Unspecified = auto()
    NoError = auto()


class ErrorResult(Exception):
    def __init__(self, ecode: ErrorCode, emsg: Optional[str]) -> None:
        self._error_code = ecode
        self._error_message = emsg

    @property
    def error_code(self) -> ErrorCode:
        return self._error_code

    @property
    def error_message(self) -> str | None:
        return self._error_message
