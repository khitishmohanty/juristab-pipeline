import time

from core.scraping import perform_click, scrape_configured_data


def process_pagination_loop(driver, step, db_engine, parent_url_id, navigation_path_parts, start_page, job_state, destination_tablename):
    """Dedicated function to handle the pagination loop, including fast-forwarding."""
    page_counter = start_page
    
    if page_counter > 1:
        print(f"\n--- Fast-forwarding to resume from page {page_counter} ---")
        for page_num_to_click in range(2, page_counter):
            print(f"  - Clicking to page {page_num_to_click}...")
            page_locator = {"type": "xpath", "value": step['page_number_xpath_template'].format(page_num=page_num_to_click)}
            click_result = perform_click(driver, page_locator, is_pagination=True)
            if click_result == "browser_crash": return False
            if click_result is None:
                fallback_locator = {"type": "xpath", "value": step['next_button_fallback_xpath']}
                if perform_click(driver, fallback_locator, is_pagination=True) == "browser_crash": return False
            time.sleep(1)

    while True:
        print(f"\n--- Scraping results on page {page_counter} ---")
        for loop_step in step['loop_steps']:
            if not scrape_configured_data(driver, loop_step['target']['value'], loop_step['scraping_config'], db_engine, parent_url_id, navigation_path_parts, page_counter, job_state, destination_tablename):
                print(f"--- Stopping pagination loop due to a scraping error on page {page_counter} ---")
                return False

        next_page_to_click = page_counter + 1
        page_locator = {"type": "xpath", "value": step['page_number_xpath_template'].format(page_num=next_page_to_click)}
        clicked_text = perform_click(driver, page_locator, is_pagination=True)
        
        if clicked_text == "browser_crash": return False
        if clicked_text is not None:
            page_counter += 1
            continue
        
        fallback_locator = {"type": "xpath", "value": step['next_button_fallback_xpath']}
        clicked_text = perform_click(driver, fallback_locator, is_pagination=True)
        
        if clicked_text == "browser_crash": return False
        if clicked_text is not None:
            page_counter += 1
            continue
        
        print("  - Could not find next page number or 'Next' button. Pagination complete.")
        break
    print("--- Numeric pagination loop finished ---")
    return True


def process_step(driver, step, db_engine, parent_url_id, navigation_path_parts, is_resuming, job_state, destination_tablename, current_page=1):
    """
    Main dispatcher function. Processes a single step from the configuration.
    """
    action = step.get('action')
    print(f"\nProcessing Step: {step.get('description', action)}")

    if action == 'click':
        if is_resuming:
            print("  - In resume mode, skipping initial navigation click.")
            return True
        clicked_text = perform_click(driver, step.get('target'))
        if clicked_text == "browser_crash": return False
        # The navigation path is pre-built, so we don't append here
        return True

    elif action == 'numeric_pagination_loop':
        return process_pagination_loop(driver, step, db_engine, parent_url_id, navigation_path_parts, current_page, job_state, destination_tablename)

    elif action == 'process_results':
        scraping_config = step.get('scraping_config')
        if not scraping_config:
            print("  - FATAL ERROR: 'process_results' action requires a 'scraping_config' object.")
            return False
        return scrape_configured_data(driver, step['target']['value'], scraping_config, db_engine, parent_url_id, navigation_path_parts, current_page, job_state, destination_tablename)
    else:
        print(f"  - WARNING: Unknown action type '{action}'. Skipping.")
    return True