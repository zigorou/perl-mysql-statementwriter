package MySQL::StatementWriter::Prepared;

use strict;
use warnings;
use parent qw(Class::Accessor::Lite);

use Carp;
use DBI qw(:sql_types);
use IO::Handle;
use SQL::Tokenizer qw(tokenize_sql);
use String::Random qw(random_regex);

my @FIELDS = qw{
    binds
    finished
    name
    statement
    tokenized
    writer
};

__PACKAGE__->mk_accessors(@FIELDS);

sub new {
    my $class  = shift;
    my $writer = shift;
    my $args   = ref $_[0] eq "HASH" ? $_[0] : { @_ };

    %$args = (
        name        => undef,
        statement   => undef,
        binds       => [],
        %$args,
        writer      => $writer,
        tokenized   => undef,
        finished    => 0,
    );

    if ($writer->server_side_prepare) {
        $args->{name} ||= random_regex(q/[A-Za-z][A-Za-z]{3,9}/);
    }

    bless $args => $class;
}

sub prepare {
    my ($self, $statement) = @_;
    my $writer = $self->{writer};

    if ($writer->server_side_prepare) {
        my $query = join " " => (
            "PREPARE",
            $self->{name},
            "FROM",
            $writer->quote($statement, SQL_VARCHAR)
        );

        $writer->write($query);
    }

    $self->{statement} = $statement;
}

sub bind_param {
    my ($self, $num, $bind_value, $opts) = @_;
    $opts ||= {};
    %$opts = (
        type => SQL_VARCHAR,
        name => '@p' . $num,
        %$opts,
    );
    if (index($opts->{name}, '@') != 0) {
        $opts->{name} = '@' . $opts->{name};
    }

    if ($num < 1) {
        croak sprintf("Invalid array index (num: %s)", $num);
    }

    my $sql_type = $opts->{type};
    my $name     = $opts->{name};
    my $writer   = $self->{writer};

    if ($writer->server_side_prepare) {
        my $query = join " " => (
            "SET",
            $name,
            "=",
            ref $bind_value eq "SCALAR" ? 
                $$bind_value : $writer->quote($bind_value, $sql_type),
        );
        $writer->write($query);
    }

    $self->{binds}[$num - 1] = {
        value => $bind_value,
        type  => $sql_type,
        name  => $name,
    };

    1;
}

sub execute {
    my ($self, @binds) = @_;

    my $i = 1;
    for my $bind (@binds) {
        my ($bind_value, $opts) = (ref $bind eq 'HASH') ? 
            ( $bind->{value}, { type => $bind->{type} } ) : ( $bind, {} );
        $self->bind_param($i++, $bind_value, $opts);
    }

    my $writer = $self->{writer};
    if ($writer->server_side_prepare) {
        my $query = join " " => (
            "EXECUTE",
            $self->{name},
            "",
        );

        my $bind_num = @{$self->{binds}};
        if ($bind_num > 0) {
            $query .= join " " => (
                "USING",
                join ", " => (
                    map { $self->{binds}[$_]{name} } ( 0 .. $bind_num - 1 )
                )
            );
        }

        $writer->write($query);
    }
    else {
        my $query = "";
        my @tokens = do {
            unless (ref $self->{tokenized} eq "ARRAY") {
                my @tokens = tokenize_sql($self->{statement});
                $self->{tokenized} = [ @tokens ];
            }
            @{$self->{tokenized}};
        };
        my @binds = @{$self->{binds}};

        for my $token (@tokens) {
            if ($token eq "?") {
                unless (@binds > 0) {
                    croak "Insufficient bind parameters";
                }
                my $bind = shift @binds;
                $query .= ref $bind->{value} eq "SCALAR" ? 
                    ${$bind->{value}} : $writer->quote(@$bind{qw/value type/});
            }
            else {
                $query .= $token;
            }
        }

        if (@binds > 0) {
            croak "Too many bind parameters";
        }

        $writer->write($query);
    }
}

sub finish {
    my $self = shift;
    my $writer = $self->{writer};
    if (!$self->{finished} && $writer->server_side_prepare) {
        my $query = join " " => (
            "DEALLOCATE PREPARE",
            $self->{name}
        );
        $writer->write($query);
        $self->{finished} = 1;
    }
}

sub DESTROY {
    shift->finish;
}

1;
__END__

=encoding utf8

=head1 NAME

MySQL::StatementWriter::Prepared - ...

=head1 SYNOPSIS

  use MySQL::StatementWriter::Prepared;

=head1 DESCRIPTION

MySQL::StatementWriter::Prepared is

B<THIS IS A DEVELOPMENT RELEASE. API MAY CHANGE WITHOUT NOTICE>.

=head1 AUTHOR

Toru Yamaguchi E<lt>zigorou@cpan.orgE<gt>

=head1 SEE ALSO

=head1 LICENSE

Copyright (C) Toru Yamaguchi

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
