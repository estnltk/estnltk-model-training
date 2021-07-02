import luigi

from luigi.parameter import ParameterVisibility
# from luigi.parameter import _no_value


class ObjectParameter(luigi.Parameter):
    """
    A way to specify that the parameter of the Luigi task is an object without problems.

    A parameter to a Luigi task can be any object. However, Luigi makes a great effort to log what are
    the parameter values and also lets you to specify the parameter through the command-line.
    This is real nuisance when you want to define task parameters that could be specified only in the code:

    * By default luigi invokes parameter.__repr__() to serialise parameter value. This value is used in logs.
      So the task description may blow up if you want to use a class with complex __repr__ function.
      The code in this class outputs only a class name without calling __repr__().

    * Luigi command-line parser attempts to initialise parameter from a command-line argument.
      For doing that it call a special parse method to create the object from string description.
      The code in this class blocks that nonsense and raises an exception instead.

    * It also prevents luigi from storing the parameter value in its internal logs, i.e., the parameter value
      is visible only inside the task.

    BUG: The automatic target naming in CDA tasks ignores the value.
         Reason: ParameterVisibility.PRIVATE and thus it Task.to_str_params(...) always ignores the parameter
         Even if we set ParameterVisibility.HIDDEN we get wrong result as serialisation provides only class name

    SOLUTION: Either it is a WTF feature or we have to redefine CDATask.hash function to loop over private parameters
              and use class hash instead of default parameter serialisation. Nothing hard but still things to do

    """

    def __init__(self, default = luigi.parameter._no_value, significant: bool = True, positional: bool = True):
        """
        You can specify three properties of the parameter in the constructor:
        :param default:     the default value for this parameter. This can be any object.
        :param significant: whether the parameter is significant. A parameter is significant if two tasks with
                            different parameter value lead to the same outcome. Password is a good example of
                            insignificant parameter. Keep it significant unless you know what you do!
        :param positional:  you must write the argument name if the value is false.
        """

        # noinspection PyTypeChecker
        super().__init__(
            default=default,                         #
            significant=significant,                 #
            description=None,                        # No description as it is not a commandline parameter
            positional=positional,                   #
            always_in_help=False,                    # Not to be present in the command line help
            batch_method=None,                       # You cannot combine different objects into single object
            visibility=ParameterVisibility.PRIVATE   # The parameter  visible only inside the task
        )

    def parse(self, input_text):
        raise luigi.parameter.ParameterException(
            """
            ObjectParameter cannot be specified through command line. 
            Trace the value '{}' among command line arguments to find the offending parameter""".format(input_text))

    def serialize(self, parameter):
        return '{object}(...)'.format(object=parameter.__class__.__name__)
