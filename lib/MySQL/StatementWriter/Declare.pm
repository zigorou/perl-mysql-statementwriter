package MySQL::StatementWriter::Declare;

use strict;
use warnings;

use Carp;
use MySQL::StatementWriter;
use MySQL::StatementWriter::Prepared;

our @EXPORT = qw(
    mysql_writer
    fh
    delimiter
    new_line
    server_side_prepare
    begin_work
    commit
    rollback
    do
    prepare
);

sub import {
    my $class = shift;
    my $pkg   = caller;

    no strict 'refs';
    no warnings 'redefine';

    *{"$pkg\::mysql_writer"}        = *mysql_writer;
    *{"$pkg\::fh"}                  = sub { goto &fh; };
    *{"$pkg\::delimiter"}           = sub { goto &delimiter; };
    *{"$pkg\::new_line"}            = sub { goto &new_line; };
    *{"$pkg\::server_side_prepare"} = sub { goto &server_side_prepare; };
    *{"$pkg\::txn"}                 = sub (&@) { goto &txn };
    *{"$pkg\::begin_work"}          = sub { goto &begin_work; };
    *{"$pkg\::commit"}              = sub { goto &commit; };
    *{"$pkg\::rollback"}            = sub { goto &rollback; };
    *{"$pkg\::do_query"}            = sub { goto &do_query; };
    *{"$pkg\::prepare"}             = sub (&@) { goto &prepare; };
    *{"$pkg\::name"}                = sub { goto &name; };
    *{"$pkg\::statement"}           = sub { goto &statement; };
}

sub __stub {
    my $func = shift;
    return sub {
        croak "Can't call $func() outside mysql_writer block";
    };
}

*fh                  = __stub "fh";
*delimiter           = __stub "delimiter";
*new_line            = __stub "new_line";
*server_side_prepare = __stub "server_side_prepare";
*txn                 = __stub "txn";
*begin_work          = __stub "begin_work";
*commit              = __stub "commit";
*rollback            = __stub "rollback";
*do_query            = __stub "do_query";
*prepare             = __stub "prepare";
*name                = __stub "name";
*statement           = __stub "statement";

sub mysql_writer (&@) {
    my $block = shift;
    my $writer = MySQL::StatementWriter->new();
    no strict 'refs';
    no warnings 'redefine';
    local *fh = sub { $writer->fh(shift); };
    local *delimiter = sub { $writer->delimiter(shift); };
    local *new_line = sub { $writer->new_line(shift); };
    local *server_side_prepare = sub { $writer->server_side_prepare(shift); };
    local *txn = sub (&@) {
        my $code = shift;
        $writer->begin_work;
        $code->();
        $writer->commit;
    };
    local *begin_work = sub { $writer->begin_work; };
    local *commit     = sub { $writer->commit; };
    local *rollback   = sub { $writer->rollback; };
    local *do_query = sub { 
        $writer->do(@_); 
    };
    local *prepare = sub { 
        my $code = shift;
        my %args;
        local *name = sub {
            $args{name} = shift;
        };
        local *statement = sub {
            $args{statement} = shift;
        };
        $code->();
        $writer->prepare(@args{qw/statement name/});
    };
    $block->();
    return $writer;
}

1;
__END__

=encoding utf8

=head1 NAME

MySQL::StatementWriter::Declare - ...

=head1 SYNOPSIS

  use MySQL::StatementWriter::Declare;

=head1 DESCRIPTION

MySQL::StatementWriter::Declare is

B<THIS IS A DEVELOPMENT RELEASE. API MAY CHANGE WITHOUT NOTICE>.

=head1 AUTHOR

Toru Yamaguchi E<lt>zigorou@cpan.orgE<gt>

=head1 SEE ALSO

=head1 LICENSE

Copyright (C) Toru Yamaguchi

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
