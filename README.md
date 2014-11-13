Storm-Simple-Crawler
====================

A simple crawler based on storm/kafka/redis

Features for version 0.0.1:
* Being Polite : Robot Rules and URL partition by Host
* KAFKA as Spout and output destination.
* Redis as backend for bloomfilter for URL de-duplication.
* Simple crawler: just crawl given depths and topn urls.

Roadmap:
1. Xpath extractor.
2. HTMLUNIT integration.

