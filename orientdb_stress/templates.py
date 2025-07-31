import importlib.resources
import logging
from pathlib import Path
from typing import Any, Dict, Optional

import jinja2


class Template:
    LOG = logging.getLogger("Template")

    def __init__(self, name: str, template_content: str) -> None:
        self.name = name
        self.template_content = template_content
        self.jinja_template = jinja2.Template(template_content)

    def render(self, context: Dict[str, Any]) -> str:
        return self.jinja_template.render(**context)

    def generate(self, target_file: Path, context: Optional[Dict[str, Any]] = None) -> None:
        Template.LOG.debug("Generating file %s from template %s", target_file, self.name)
        with open(target_file, "wt", encoding="UTF-8") as output_file:
            if context is not None:
                output_file.write(self.render(context))
            else:
                output_file.write(self.template_content)


class Templates:
    LOG = logging.getLogger("Templates")

    @staticmethod
    def get_template_dir() -> Path:
        try:
            # First try using importlib.resources for installed packages
            with importlib.resources.path("orientdb_stress", "templates") as template_path:
                return template_path
        except (ImportError, ModuleNotFoundError):
            # Fall back to relative path for development environment
            module_dir = Path(__file__).parent
            return module_dir / "templates"

    @staticmethod
    def load_template(template_name: str) -> Template:
        template_dir = Templates.get_template_dir()
        template_path = template_dir / template_name

        Templates.LOG.debug("Loading template %s from %s", template_name, template_path)

        if not template_path.exists():
            raise FileNotFoundError(f"Template {template_name} not found at {template_path}")

        with open(template_path, "rt", encoding="UTF-8") as template_file:
            template_content = template_file.read()
            return Template(template_name, template_content)

    @staticmethod
    def load_template_from_string(template_name: str, template_content: str) -> Template:
        return Template(template_name, template_content)
