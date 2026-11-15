"""FailSlice: shrink multi-table data to a small, FK-consistent, verified reproducer.

Typical use::

    import failslice

    project = failslice.load_project("failslice.yaml")
    report = failslice.check(project)          # FK orphans, predicate determinism
    result = failslice.reduce(project)         # reduce, export, replay in a fresh process
    print(result.minimality_label, result.verification.status)

See ``docs/design.md`` for the guarantees and their limits.
"""

__version__ = "0.0.1.dev0"

from failslice.api import CheckReport, ReductionResult, check, reduce, validate_predicate, verify
from failslice.config import ConfigError, Project, load_project
from failslice.reducer import (
    Budget,
    DeterminismError,
    Minimality,
    PredicateNotFailingError,
    ProgressEvent,
    StopReason,
)
from failslice.safety import PredicateValidationError
from failslice.verify import VerificationReport

__all__ = [
    "Budget",
    "CheckReport",
    "ConfigError",
    "DeterminismError",
    "Minimality",
    "PredicateNotFailingError",
    "PredicateValidationError",
    "ProgressEvent",
    "Project",
    "ReductionResult",
    "StopReason",
    "VerificationReport",
    "__version__",
    "check",
    "load_project",
    "reduce",
    "validate_predicate",
    "verify",
]
