import calendar
import json
import os
import time
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

REQUEST_FILE = "recurring_expenses_request.json"
DONE_FILE = "recurring_expenses_done.json"
ERROR_FILE = "recurring_expenses_error.json"

POLL_INTERVAL = 0.10


def safe_remove(path: str) -> None:
    try:
        os.remove(path)
    except FileNotFoundError:
        pass


def read_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as file:
        return json.load(file)


def write_json(path: str, data: Dict[str, Any]) -> None:
    with open(path, "w", encoding="utf-8") as file:
        json.dump(data, file, indent=2)


def parse_target_month(value: str) -> Tuple[int, int]:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("targetMonth must be a non-empty string in YYYY-MM format.")

    parts = value.strip().split("-")
    if len(parts) != 2:
        raise ValueError("targetMonth must be in YYYY-MM format.")

    try:
        year = int(parts[0])
        month = int(parts[1])
    except ValueError as exc:
        raise ValueError("targetMonth must be in YYYY-MM format.") from exc

    if month < 1 or month > 12:
        raise ValueError("targetMonth month must be between 1 and 12.")

    return year, month


def parse_month_string(value: Optional[str], field_name: str) -> Optional[Tuple[int, int]]:
    if value is None:
        return None
    return parse_target_month(value)


def month_to_int(year: int, month: int) -> int:
    return year * 100 + month


def safe_float(value: Any, field_name: str) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be numeric.") from exc

    return number


def safe_int(value: Any, field_name: str) -> int:
    try:
        number = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be an integer.") from exc

    return number


def load_json_object_or_list(path: str) -> Any:
    if not os.path.exists(path):
        raise FileNotFoundError(f"JSON file not found: {path}")
    return read_json(path)


def load_rules(path: str) -> List[Dict[str, Any]]:
    data = load_json_object_or_list(path)

    if isinstance(data, dict):
        rules = [data]
    elif isinstance(data, list):
        rules = data
    else:
        raise ValueError("Rules JSON must be an object or a list of objects.")

    validated_rules: List[Dict[str, Any]] = []
    for rule in rules:
        if not isinstance(rule, dict):
            raise ValueError("Every recurring rule must be a JSON object.")
        validated_rules.append(validate_and_normalize_rule(rule))

    return validated_rules


def load_existing_expenses(path: str) -> List[Dict[str, Any]]:
    data = load_json_object_or_list(path)

    if not isinstance(data, list):
        raise ValueError("Existing expenses JSON must be a list.")

    expenses: List[Dict[str, Any]] = []
    for item in data:
        if isinstance(item, dict):
            expenses.append(item)

    return expenses


def validate_and_normalize_rule(rule: Dict[str, Any]) -> Dict[str, Any]:
    required_fields = ["ruleId", "amount", "category", "dayOfMonth"]
    missing_fields = [field for field in required_fields if field not in rule]
    if missing_fields:
        raise ValueError(f"Recurring rule missing field(s): {', '.join(missing_fields)}")

    rule_id = str(rule["ruleId"]).strip()
    category = str(rule["category"]).strip()

    if not rule_id:
        raise ValueError("ruleId must be a non-empty string.")
    if not category:
        raise ValueError("category must be a non-empty string.")

    amount = safe_float(rule["amount"], "amount")
    if amount <= 0:
        raise ValueError("amount must be greater than 0.")

    day_of_month = safe_int(rule["dayOfMonth"], "dayOfMonth")
    if day_of_month < 1 or day_of_month > 31:
        raise ValueError("dayOfMonth must be between 1 and 31.")

    start_month = parse_month_string(rule.get("startMonth"), "startMonth")
    end_month = parse_month_string(rule.get("endMonth"), "endMonth")

    if start_month and end_month:
        if month_to_int(*start_month) > month_to_int(*end_month):
            raise ValueError("startMonth cannot be after endMonth.")

    description = str(rule.get("description", "")).strip()

    return {
        "ruleId": rule_id,
        "amount": round(amount, 2),
        "category": category,
        "dayOfMonth": day_of_month,
        "description": description,
        "startMonth": start_month,
        "endMonth": end_month
    }


def validate_request(request_data: Dict[str, Any]) -> None:
    required_fields = ["rulesJsonPath", "existingExpensesPath", "targetMonth"]
    missing_fields = [field for field in required_fields if field not in request_data]
    if missing_fields:
        raise ValueError(f"Missing required field(s): {', '.join(missing_fields)}")

    for field_name in ("rulesJsonPath", "existingExpensesPath", "targetMonth"):
        if not isinstance(request_data[field_name], str) or not request_data[field_name].strip():
            raise ValueError(f"{field_name} must be a non-empty string.")

    parse_target_month(request_data["targetMonth"])


def rule_applies_to_month(rule: Dict[str, Any], target_year: int, target_month: int) -> bool:
    target_value = month_to_int(target_year, target_month)

    if rule["startMonth"] is not None and target_value < month_to_int(*rule["startMonth"]):
        return False

    if rule["endMonth"] is not None and target_value > month_to_int(*rule["endMonth"]):
        return False

    return True


def build_occurrence_date(target_year: int, target_month: int, day_of_month: int) -> str:
    last_day = calendar.monthrange(target_year, target_month)[1]
    actual_day = min(day_of_month, last_day)
    return date(target_year, target_month, actual_day).isoformat()


def build_existing_key_set(expenses: List[Dict[str, Any]]) -> set[Tuple[str, str]]:
    keys: set[Tuple[str, str]] = set()

    for expense in expenses:
        rule_id = str(expense.get("ruleId", "")).strip()
        occurrence_date = str(expense.get("occurrenceDate", "")).strip()

        if rule_id and occurrence_date:
            keys.add((rule_id, occurrence_date))

    return keys


def build_generated_entry(rule: Dict[str, Any], occurrence_date: str) -> Dict[str, Any]:
    created_at = f"{occurrence_date}T00:00:00"

    entry = {
        "amount": rule["amount"],
        "category": rule["category"],
        "created_at": created_at,
        "ruleId": rule["ruleId"],
        "occurrenceDate": occurrence_date,
        "recurringGenerated": True
    }

    if rule["description"]:
        entry["description"] = rule["description"]

    return entry


def generate_expenses(
    rules: List[Dict[str, Any]],
    existing_expenses: List[Dict[str, Any]],
    target_year: int,
    target_month: int
) -> List[Dict[str, Any]]:
    existing_keys = build_existing_key_set(existing_expenses)
    generated_entries: List[Dict[str, Any]] = []

    for rule in sorted(rules, key=lambda item: item["ruleId"].lower()):
        if not rule_applies_to_month(rule, target_year, target_month):
            continue

        occurrence_date = build_occurrence_date(target_year, target_month, rule["dayOfMonth"])
        key = (rule["ruleId"], occurrence_date)

        if key in existing_keys:
            continue

        generated_entries.append(build_generated_entry(rule, occurrence_date))

    return generated_entries


def process_one_request() -> None:
    safe_remove(DONE_FILE)
    safe_remove(ERROR_FILE)

    request_data = read_json(REQUEST_FILE)
    if not isinstance(request_data, dict):
        raise ValueError("Request file must contain a JSON object.")

    validate_request(request_data)

    rules_json_path = request_data["rulesJsonPath"].strip()
    existing_expenses_path = request_data["existingExpensesPath"].strip()
    target_year, target_month = parse_target_month(request_data["targetMonth"].strip())

    rules = load_rules(rules_json_path)
    existing_expenses = load_existing_expenses(existing_expenses_path)
    generated_entries = generate_expenses(rules, existing_expenses, target_year, target_month)

    response = {
        "status": "ok",
        "targetMonth": f"{target_year:04d}-{target_month:02d}",
        "generatedCount": len(generated_entries),
        "generatedExpenses": generated_entries
    }

    write_json(DONE_FILE, response)


def run_service() -> None:
    print("Recurring Expenses Service running.")
    print(f"Watching for: {REQUEST_FILE}")

    while True:
        try:
            if os.path.exists(REQUEST_FILE):
                try:
                    process_one_request()
                except json.JSONDecodeError:
                    write_json(ERROR_FILE, {"status": "error", "message": "Invalid JSON in request file."})
                except Exception as exc:
                    write_json(ERROR_FILE, {"status": "error", "message": str(exc)})
                finally:
                    safe_remove(REQUEST_FILE)

            time.sleep(POLL_INTERVAL)
        except KeyboardInterrupt:
            print("\nService stopped.")
            break


if __name__ == "__main__":
    run_service()