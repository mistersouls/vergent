# Copyright 2026 Tourillon Contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Bootstrap entry point for the Tourillon CLI.

Delegates unconditionally to the root Typer application assembled in
``tourillon.infra.cli.main``. This module exists so that the bootstrap
package can be invoked as a standalone script during early-stage node
initialisation without duplicating any command registration logic.
"""

from tourillon.infra.cli.main import app


def main() -> None:
    """Invoke the Tourillon CLI application."""
    app()
