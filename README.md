# coli-rich-rvk-bk

Anreicherung von K10plus-Datensätzen durch BK-Notationen auf Grundlage von vorhandener RVK-Erschließung.

## Requirements

Perl and additional modules `URI`, `IO::Socket::SSL`, and `DBD::SQLite`:

~~~sh
sudo apt-get install liburi-perl libio-socket-ssl-perl libdbd-sqlite3-perl
~~~

## Usage

### Get PPNs to enrich

Given a full PICA+ database dump, extract PPNs of records having RVK but not BK (also requires `pica-rs`):

~~~
zcat kxp_ohne_expansion.dat.gz | pica filter '045R? && !045Q/01?'| pica select '003@.0' > rvk-no-bk.ppn
~~~

### Enrichment

Generate up to 1000 enrichments:

~~~
./coli-rich.pl --limit 1000 < rvk-no-bk.ppn
~~~

Lookup of mappings are cached in local cache file `cache.db`.

