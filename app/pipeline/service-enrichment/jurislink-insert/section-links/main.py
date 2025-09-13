from src.juris_link_extractor import JurisLinkExtractor

def main():
    """
    Main function to start the juris link extraction process.
    """
    print("Starting Juris Link Extraction Process...")
    extractor = JurisLinkExtractor(config_path='config/config.yaml')
    extractor.process_source_ids()
    print("Juris Link Extraction Process Finished.")

if __name__ == "__main__":
    main()
