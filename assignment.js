import fs from "fs/promises";
import { parseXMLFeed } from "./src/parser.js";
import { createBatcher } from "./src/batcher.js";
import ExternalService from "./src/external-service.js";

const FEED_FILE_PATH = process.argv[2] || "./data/feed.xml";
const MAX_BATCH_SIZE_MB = 5;

async function main() {
  try {
    console.log(`Reading feed from: ${FEED_FILE_PATH}`);

    // Read and parse XML feed (async)
    const xmlContent = await fs.readFile(FEED_FILE_PATH, "utf-8");

    console.log("Parsing XML feed...");
    const products = parseXMLFeed(xmlContent);

    // Validate products array
    if (!products || products.length === 0) {
      console.warn("No products found in feed");
      console.log("Processing complete!");
      return;
    }

    console.log(`Found ${products.length.toLocaleString()} products in feed\n`);

    const externalService = ExternalService();

    // Create batcher and process products
    const batcher = createBatcher(MAX_BATCH_SIZE_MB, externalService);
    try {
      batcher.addProducts(products);
    } finally {
      batcher.flush();
    }

    console.log("Processing complete!");
  } catch (error) {
    if (error.code === "ENOENT") {
      console.error(`Error: Feed file not found: ${FEED_FILE_PATH}`);
    } else {
      console.error("Error:", error.message);
    }
    process.exit(1);
  }
}

main();
