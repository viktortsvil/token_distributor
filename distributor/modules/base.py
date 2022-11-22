import logging
import distributor.modules.pipelines as pp


def main(pipeline: str):
    if pipeline == 'new_students':
        pp.new_students()
    elif pipeline == 'monthly_active_students':
        pp.active_students()
    else:
        logging.info(f"Pipeline '{pipeline}' not found")
