from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from quant_research.contracts.bar import Frequency
from quant_research.factors.contracts import FactorSpec


class DuplicateFactorError(ValueError):
    pass


class UnknownFactorError(KeyError):
    pass


class UnsupportedFrequencyError(ValueError):
    pass


@dataclass(frozen=True)
class RegisteredFactor:
    spec: FactorSpec
    compute: Any


class FactorRegistry:
    def __init__(self):
        self._factors: dict[tuple[str, str], RegisteredFactor] = {}

    def register(self, spec: FactorSpec, compute: Any) -> None:
        key = (spec.factor_id, spec.version)
        if key in self._factors:
            raise DuplicateFactorError(
                f"factor already registered: {spec.factor_id}@{spec.version}"
            )
        self._factors[key] = RegisteredFactor(spec=spec, compute=compute)

    def get(self, factor_id: str, version: str | None = None) -> RegisteredFactor:
        if version is not None:
            key = (factor_id, version)
            try:
                return self._factors[key]
            except KeyError as exc:
                raise UnknownFactorError(f"unknown factor: {factor_id}@{version}") from exc

        matches = [
            registered
            for (registered_factor_id, _), registered in self._factors.items()
            if registered_factor_id == factor_id
        ]
        if not matches:
            raise UnknownFactorError(f"unknown factor: {factor_id}")
        if len(matches) > 1:
            versions = ", ".join(sorted(match.spec.version for match in matches))
            raise ValueError(f"factor version is ambiguous for {factor_id}: {versions}")
        return matches[0]

    def resolve_many(self, factor_ids: tuple[str, ...], *, freq: Frequency) -> list[RegisteredFactor]:
        resolved = [self.get(factor_id) for factor_id in factor_ids]
        unsupported = [
            factor.spec.factor_id for factor in resolved if freq not in factor.spec.supported_freqs
        ]
        if unsupported:
            raise UnsupportedFrequencyError(
                f"factors do not support freq {freq.value}: {', '.join(unsupported)}"
            )
        return resolved

    def list(self, *, namespace: str | None = None, tag: str | None = None) -> list[FactorSpec]:
        specs = [registered.spec for registered in self._factors.values()]
        if namespace is not None:
            specs = [spec for spec in specs if spec.namespace == namespace]
        if tag is not None:
            specs = [spec for spec in specs if tag in spec.tags]
        return sorted(specs, key=lambda spec: (spec.factor_id, spec.version))
