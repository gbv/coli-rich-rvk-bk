#!/usr/bin/env perl
use v5.20;

use HTTP::Tiny;
use JSON::PP 'decode_json';
use URI;    # non-core dependency but likely installed
use Time::HiRes;

my $rvkScheme = "http://bartoc.org/en/node/533";
my $bkScheme  = "http://bartoc.org/en/node/18785";
my $narrowOrExact =
"http://www.w3.org/2004/02/skos/core#exactMatch|http://www.w3.org/2004/02/skos/core#narrowMatch";

sub error {
    say STDERR @_;
}

sub fetch {
    my $url = URI->new(shift);
    $url->query_form(@_);
    my $res = HTTP::Tiny->new()->get("$url");
    if ( $res->{success} ) {
        return decode_json( $res->{content} );
    }
    else {
        error("Failed HTTP request: $url");
        return;
    }
}

sub enrichment {
    my @rvkuris = map { s/ /%20/g; "http://rvk.uni-regensburg.de/nt/$_" } @_;

    # TODO: cache results to not query same RVK again and again
    my $infer = fetch(
        "https://coli-conc.gbv.de/api/mappings/infer",
        fromScheme => $rvkScheme,
        from       => join( "|", @rvkuris ),
        type       => $narrowOrExact,
        toScheme   => $bkScheme,
        partOf     => 'any',
        strict     => 1
    );

    my %enrich;
    for (@$infer) {
        my $notation = $_->{to}{memberSet}[0]{notation}[0];
        my $mapping  = $_->{uri} ? $_ : $_->{source}[0];
        my $uri      = $mapping->{uri};

        if ( $enrich{$notation} ) {
            push @{ $enrich{$notation} }, $uri;
        }
        else {
            $enrich{$notation} = [$uri];
        }
    }

    return %enrich;
}

while ( my $ppn = <> ) {
    Time::HiRes::sleep(0.05);    # avoid too many requests

    chomp $ppn;
    unless ( $ppn =~ /^[0-9]+[0-9X]$/ ) {
        error("Invalid PPN at line $.");
        next;
    }

    my $subj = fetch(
        "https://coli-conc.gbv.de/subjects-k10plus/subjects",
        scheme => "$rvkScheme|$bkScheme",
        record => "http://uri.gbv.de/document/opac-de-627:ppn:$ppn",
        live   => 1
    );
    unless ($subj) {
        error("No subjects found for PPN $ppn at line $.");
        next;
    }

    my @bk = map { $_->{notation}[0] }
      grep { $_->{inScheme}[0]{uri} eq $bkScheme } @$subj;
    my @rvk = map { $_->{notation}[0] }
      grep { $_->{inScheme}[0]{uri} eq $rvkScheme } @$subj;

    unless (@rvk) {
        error("Record $ppn has no RVK");
        next;
    }
    if (@bk) {
        error( "Record $ppn already has BK " . join( "|", @bk ) );
        next;
    }

    if ( my %enrich = enrichment(@rvk) ) {
        say "  003@\$0$ppn";
        for my $bk ( keys %enrich ) {
            say "+ 045Q/01 \$a$bk\$Acoli-conc"
              . join( "", map { '$A' . $_ } @{ $enrich{$bk} } );
        }
        say;
    }
    else {
        error("No enrichment found for PPN $ppn");
        next;
    }
}
