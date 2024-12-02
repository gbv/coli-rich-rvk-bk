# coli-rich-rvk-bk

Anreicherung von K10plus-Datensätzen durch BK-Notationen auf Grundlage von vorhandener RVK-Erschließung.

## Requirements

Perl and additional modules `URI` and `IO::Socket::SSL`:

~~~sh
sudo apt-get install liburi-perl libio-socket-ssl-perl
~~~

## Get PPNs to enrich

Given a full PICA+ database dump, extract PPNs of records having RVK but not BK using `pica-rs`:

~~~
zcat kxp_ohne_expansion.dat.gz | pica filter '045R? && !045Q/01?'| pica select '003@.0' > rvk-no-bk.ppn
~~~
