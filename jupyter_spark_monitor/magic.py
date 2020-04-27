# -*- coding: utf-8 -*-
from pyspark.sql import DataFrame
from IPython.core import magic_arguments
from IPython.core.magic import (
    Magics,
    magics_class,
    line_cell_magic,
    needs_local_scope,
)


@magics_class
class PySparkDataFrameMagic(Magics):

    @line_cell_magic
    @magic_arguments.magic_arguments()
    @magic_arguments.argument('--help', action='help', help="""Displays the help and usage text for this magic.""")
    @magic_arguments.argument('--limit', type=int, default=10, help="""Limit of DataFrame rows: (default: 10).""")
    @magic_arguments.argument('--output', type=str, default='html', help="""Output of DataFrame: html(default), csv.""")
    @needs_local_scope
    def pyspark_dataframe(self, line=None, cell=None, local_ns=None):
        try:
            args = magic_arguments.parse_argstring(self.pyspark_dataframe, line)
        except SystemExit:
            return
        if cell is None or not cell.strip():
            return
        code = "rv = {}".format(cell.strip())
        ns = {}
        glob = self.shell.user_ns
        # handles global vars with same name as local vars. We store them in conflict_globs.
        conflict_globs = {}
        if local_ns and cell is None:
            for var_name, var_val in glob.items():
                if var_name in local_ns:
                    conflict_globs[var_name] = var_val
            glob.update(local_ns)
        exec(code, glob, ns)
        rv = ns['rv']
        if conflict_globs:
            glob.update(conflict_globs)
        if not isinstance(rv, DataFrame):
            raise TypeError("value {!r} is not instance of {!r}".format(rv, DataFrame))
        if args.output == 'html':
            return rv.limit(args.limit).toPandas()
        else:
            print(rv.limit(args.limit).toPandas().to_csv(index=False))


def init_extension(ipython):
    ipython.register_magics(PySparkDataFrameMagic)
