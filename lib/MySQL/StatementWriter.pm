package MySQL::StatementWriter;

use strict;
use warnings;
use 5.008005;
use parent qw(Class::Accessor::Lite);
use DBI qw(:sql_types);
use IO::Handle;
use List::MoreUtils qw(first_index);
use MySQL::StatementWriter::Prepared;

our $VERSION = '0.01';
my @FIELDS = qw(
    fh
    is_begin_work
    new_line
    server_side_prepare
    use_delimiter_command
);
my @NON_QUOTE_TYPES = (
    SQL_TINYINT, SQL_BIGINT,
    SQL_NUMERIC .. SQL_DOUBLE,
);

__PACKAGE__->mk_accessors(@FIELDS);

sub new {
    my $class = shift;
    my $args = ref $_[0] eq "HASH" ? $_[0] : { @_ };

    %$args = (
        fh                    => undef,
        is_begin_work         => 0,
        delimiter             => ";",
        use_delimiter_command => 1,
        new_line              => "\n",
        server_side_prepare   => 0,
        %$args,
    );

    $args->{fh} ||= do {
        my $io = IO::Handle->new;
        $io->fdopen(fileno(STDOUT), "w");
        $io;
    };

    bless $args => $class;
}

sub begin_work {
    my $self = shift;
    unless ($self->{is_begin_work}) {
        $self->write("BEGIN");
        $self->{is_begin_work} = 1;
    }
}

sub do {
    my ($self, $statement, @binds) = @_;
    if (@binds > 0) {
        my $sth = MySQL::StatementWriter::Prepared->new($self, { name => "tmp_stmt" });
        $sth->prepare($statement);
        $sth->execute(@binds);
        $sth->finish;
    }
    else {
        $self->write($statement);
    }
}

sub prepare {
    my ($self, $statement, $name) = @_;
    my $sth = MySQL::StatementWriter::Prepared->new($self, {
        name      => $name,
    });
    $sth->prepare($statement);
    return $sth;
}

sub commit {
    my $self = shift;
    if ($self->{is_begin_work}) {
        $self->write("COMMIT");
        $self->{is_begin_work} = 0;
    }
}

sub rollback {
    my $self = shift;
    if ($self->{is_begin_work}) {
        $self->write("ROLLBACK");
        $self->{is_begin_work} = 0;
    }
}

sub write {
    my ($self, $query) = @_;
    $query ||= "";
    chomp($query);
    if (length $self->{delimiter}) {
        $query =~ s/$self->{delimiter}+$//s;
    }
    $query .= $self->{delimiter} . $self->{new_line};
    $self->{fh}->write($query, length $query);
}

sub delimiter {
    my ($self, $delimiter) = @_;
    if ($delimiter) {
        $self->{delimiter} = "";
        $self->write("DELIMITER " . $delimiter);
        $self->{delimiter} = $delimiter;
    }
    else {
        return $self->{delimiter};
    }
}

sub quote {
    my ($self, $bind_value, $sql_type) = @_;
    if (ref $bind_value eq 'SCALAR') {
        return $$bind_value;
    }

    if ( defined $sql_type && ( first_index { $_ == $sql_type } @NON_QUOTE_TYPES ) > -1) {
        return $bind_value;
    }

    $bind_value =~ s/'/''/g;
    return "'$bind_value'";
}

sub DESTROY {
    shift->commit;
}

1;
__END__

=encoding utf8

=head1 NAME

MySQL::StatementWriter - ...

=head1 SYNOPSIS

  use MySQL::StatementWriter;

=head1 DESCRIPTION

MySQL::StatementWriter is

B<THIS IS A DEVELOPMENT RELEASE. API MAY CHANGE WITHOUT NOTICE>.

=head1 AUTHOR

Toru Yamaguchi E<lt>zigorou@cpan.orgE<gt>

=head1 SEE ALSO

=head1 LICENSE

Copyright (C) Toru Yamaguchi

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
