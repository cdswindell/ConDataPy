from __future__ import annotations

from enum import Enum, verify, UNIQUE, auto


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
