# TODO (issue #1208):
#
#   File legacy/utils.py was introduced by moving whole common/utils.py to be it. This is in turn was done
#   to highlight the problem of duplicate configurations:
#     OLD CONF: <checkout>/conf.ini (based on conf.egcut_epi.micro.ini or similar examples under revision control)
#     NEW CONF: <checkout>/configurations/<conf-file-name>.ini (based on example egcut_epi_microrun.example.ini)
#   Those configurations overlap partially.
#
#   Needed:
#   Get rid of 'from legacy.utils import ...' (and use 'from common.utils import ...') so that
#   old configuration from <checkout>/conf.ini is not used any more at all (over the whole code base), and all the
#   existing functionality works just as well as it did so far.
import os


def write_empty_file(folder, file):
    if not os.path.exists(os.path.join(folder)):
        os.makedirs(os.path.join(folder))
    out = file.open("w")
    out.close()
