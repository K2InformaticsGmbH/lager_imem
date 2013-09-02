-module(lager_dal).

-export([ create_check_table/3
        , create_check_table/4
        , dirty_write/2
        ]).

create_check_table(Table, Columns, Opts)            -> imem_meta:create_check_table(Table, Columns, Opts).
create_check_table(Table, ColumnSpecs, Opts, Owner) -> imem_meta:create_check_table(Table, ColumnSpecs, Opts, Owner).
dirty_write(Table, Record)                          -> catch imem_meta:dirty_write(Table, Record).
