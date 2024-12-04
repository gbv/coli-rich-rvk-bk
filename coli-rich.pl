#!/usr/bin/env perl
use v5.20;

use HTTP::Tiny;
use JSON::PP qw(decode_json encode_json);
use URI;    # non-core dependency but likely installed
use DBI;
use Time::HiRes;
use Getopt::Long qw(GetOptionsFromArray);
use Pod::Usage;

my $subjectsApi = "https://coli-conc.gbv.de/subjects-k10plus/subjects";
my $rvkScheme   = "http://bartoc.org/en/node/533";
my $bkScheme    = "http://bartoc.org/en/node/18785";
my $narrowOrExact =
"http://www.w3.org/2004/02/skos/core#exactMatch|http://www.w3.org/2004/02/skos/core#narrowMatch";

my $sqlite = DBI->connect( "dbi:SQLite:dbname=cache.db", "", "",
    { AutoCommit => 1, PrintError => 1 } );
$sqlite->do(
"CREATE TABLE IF NOT EXISTS cache (rvk TEXT PRIMARY KEY, enrich TEXT, time TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL)"
);

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
    my @rvk = sort @_;
    my $key = join "|", @rvk;
    my %enrich;

    # TODO: move caching to mappings API
    my ($json) =
      $sqlite->selectrow_array("SELECT enrich FROM cache WHERE rvk='$key';");
    return %{ decode_json($json) } if $json;

    my @rvkuris =
      map { s/ /%20/g; "http://rvk.uni-regensburg.de/nt/$_" } @rvk;

    my $infer = fetch(
        "https://coli-conc.gbv.de/api/mappings/infer",
        fromScheme => $rvkScheme,
        from       => join( "|", @rvkuris ),
        type       => $narrowOrExact,
        toScheme   => $bkScheme,
        partOf     => 'any',
        strict     => 1
    );

    # TODO: if fetch fails, this should not be cached

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

    $json = encode_json( \%enrich );
    $sqlite->do("INSERT INTO cache(rvk,enrich) VALUES ('$key','$json');");

    return %enrich;
}

sub main {
    GetOptionsFromArray( \@_, \my %opt, "help|?", "limit|l:i", "sleep|s:f" )
      or pod2usage(2);
    pod2usage(1) if $opt{help};

    my $ppnCount    = 0;
    my $enrichCount = 0;
    while ( my $ppn = <STDIN> ) {
        chomp $ppn;
        unless ( $ppn =~ /^[0-9]+[0-9X]$/ ) {
            error("Invalid PPN at line $.");
            next;
        }

        Time::HiRes::sleep( $opt{sleep} ) if $ppnCount++ and $opt{sleep};

        my $subj = fetch(
            $subjectsApi,
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
            $enrichCount++;
            exit if $opt{limit} && $opt{limit} <= $enrichCount;
        }
        else {
            error("No enrichment found for PPN $ppn");
            next;
        }
    }
}

main(@ARGV) unless caller;

1;

__END__

=head1 NAME

coli-rich

=head1 SYNOPSIS

coli-rich [options] < file

Generate enrichment of BK subject indexing based in RVK subject indexing.
Expects a list of K10plus PPN from standard input. Emits PICA Patch.

=head1 OPTIONS

=over 4

=item B<--limit N>

Maximum number of enrichments to be generated (default 0 for no limit).

=item B<--sleep S>

Wait S seconds between each PPN. Can be a floating point number, e.g. 0.05.

=item B<--help>

Print this help message.

=back
