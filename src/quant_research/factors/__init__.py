"""Factor contracts, registries, DSL expressions, and compute adapters."""

from quant_research.factors.builtin import default_factor_registry, register_builtin_factors
from quant_research.factors.contracts import FactorContext, FactorRunConfig, FactorSpec

__all__ = [
    "FactorContext",
    "FactorRunConfig",
    "FactorSpec",
    "default_factor_registry",
    "register_builtin_factors",
]
