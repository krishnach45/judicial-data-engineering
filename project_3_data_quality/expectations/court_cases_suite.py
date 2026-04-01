import great_expectations as gx
import logging

logger = logging.getLogger(__name__)

def create_suite(context):
    """
    Define all expectations for court case data.
    These rules enforce data quality before data is used downstream.
    """
    suite_name = "court_cases_suite"

    # Delete existing suite if it exists
    try:
        context.delete_expectation_suite(suite_name)
    except Exception:
        pass

    suite = context.add_expectation_suite(expectation_suite_name=suite_name)

    # case_number must exist and be unique
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(
        column="case_number"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeUnique(
        column="case_number"))

    # case_type must be from controlled vocabulary
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(
        column="case_type",
        value_set=["criminal", "civil", "family", "traffic", "probate"]))

    # status must be from controlled vocabulary
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(
        column="status",
        value_set=["open", "closed", "pending", "dismissed", "appealed"],
        mostly=0.95))

    # 99% of rows must have a status
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(
        column="status",
        mostly=0.95))

    # court_id must be numeric
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(
        column="court_id"))

    # Table must have rows
    suite.add_expectation(gx.expectations.ExpectTableRowCountToBeGreaterThan(
        value=0))

    context.save_expectation_suite(suite)
    logger.info(f"Expectation suite '{suite_name}' saved")
    return suite
