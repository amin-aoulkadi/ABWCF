# Actor-Based Web Crawling Framework
The ABWCF is a customizable, distributed and scalable web crawling framework for the JVM. It is based on [Apache Pekko](https://pekko.apache.org/) and the Actor model.

The ABWCF was created and developed (up to and including commit [76b52d9](https://github.com/amin-aoulkadi/ABWCF/tree/76b52d91a131ba4f125efd9df178c77d454cc5a7)) as part of my master's thesis *Developing an Actor-Based Web Crawling Framework with Apache Pekko*.

### Features
* __Web Crawling Basics:__ The ABWCF handles basic web crawling tasks (e.g. normalizing and deduplicating URLs, fetching resources, and parsing links from fetched HTML documents). Fetched resources are processed by user-defined code to perform use case-specific tasks.
* __Polite Crawling:__ The ABWCF supports crawl delays, the Robots Exclusion Protocol (i.e. `robots.txt`), `X-Robots-Tag` HTTP headers, and `<meta name="robots">` HTML elements.
* __Crawl Limits:__ The ABWCF relies on user-defined regular expressions to filter out URLs that should not be crawled. This makes it possible to restrict crawls to certain hosts or domains. Crawls can also be limited by crawl depth.
* __Crawl Priority:__ The ABWCF includes a customizable mechanism to prioritize pages. Pages with a high crawl priority are more likely to be crawled than pages with a low crawl priority.
* __Persistence:__ The ABWCF persists which pages have already been crawled and which pages still need to be crawled. This makes it possible to pause and resume crawls.
* __Bandwidth Limits:__ The ABWCF supports configurable bandwidth limits for fetching.
* __Horizontal Scalability:__ It is possible to distribute a single crawl across multiple concurrent ABWCF instances.
* __Metrics:__ The ABWCF supports [OpenTelemetry](https://opentelemetry.io/) metrics.
* __Extensibility:__ With some knowledge of ABWCF internals and Pekko, users can add new features or replace existing ABWCF components with custom implementations.

### Limitations
* The ABWCF does not render the pages it visits, and it does not execute any JavaScript.
* The ABWCF does not support authentication (at least not out-of-the-box).

## Getting Started
Refer to the [Getting Started](https://github.com/amin-aoulkadi/ABWCF/wiki/Getting-Started) guide on the ABWCF Wiki for setup and usage instructions.

## Documentation
The [ABWCF Wiki](https://github.com/amin-aoulkadi/ABWCF/wiki) provides documentation for users and developers.

## License
This work is licensed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0).
