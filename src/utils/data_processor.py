import logging

def process_data_stream(logger):
    """Processes a simulated data stream, logging various events."""
    logger.info("--- Data Processor Utility Started ---")
    
    data_source = ["record_a", "record_b", "record_c", "record_d_bad"]
    successful_records = 0
    failed_records = 0

    for i, record in enumerate(data_source):
        logger.debug(f"Processing attempt {i+1}: Received raw record data for processing.")
        try:
            # Simulate heavy resource usage or complex calculation
            processed_data = _clean_and_transform(record)
            
            if processed_data:
                logger.info(f"Successfully processed record: {record}")
                successful_records += 1
            else:
                # Use warning level if data was processed but empty
                logger.warning(f"Record {record} was processed but resulted in Null output. Check source validity.")
                failed_records += 1
                
        except ValueError as e:
            # Use error level for expected data validation failures
            logger.error(f"Data validation failed for record '{record}'. Error: {e}", exc_info=False)
            failed_records += 1
        except Exception as e:
            # Use critical for unexpected system failure during processing
            logger.critical(f"Unexpected system failure while handling record '{record}'. Error: {e}", exc_info=True)
            failed_records += 1

    logger.info(f"Data processing finished. Summary: Successful={successful_records}, Failed={failed_records}.")
    logger.debug("Utility execution finished cleanup routine.")
    logger.info("--- Data Processor Utility Exited ---")

def _clean_and_transform(record: str) -> str | None:
    """Simulates complex cleaning and transformation logic. Raises ValueError on bad input."""
    logger = logging.getLogger(__name__)
    logger.debug(f"Starting transformation run for raw data: {record}")
    
    if not record or record == "record_b":
        raise ValueError("Record content cannot be zero or null.")

    if record == "record_d_bad":
        # Simulation of a transformation failure requiring specific handling
        # We simulate a failure that the calling code handles (ValueError)
        raise ValueError("Invalid format structure detected in payload.")
    
    # Simulate successful transformation
    transformed = record.upper().replace("RECORD", "DATA")
    logger.debug(f"Transformation successful for {record}. Result: {transformed}")
    return transformed