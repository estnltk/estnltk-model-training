import luigi

from luigi.parameter import ParameterVisibility


class PasswordParameter(luigi.Parameter):
    """
    A way to specify password parameters for Luigi tasks without leaking them into logs.

    Luigi likes to log parameter values by default which obviously leaks passwords unless you configure
    parameter options in right way. This class provides a right configuration that is safe to use.

    IMPORTANT: If the task initialisation fails then Luigi spits out all arguments. As a result, the value
    of the password is still leaked to the logs unless you specify the password as object instead of str.
    The class PasswordValue acts as string but is safe against such leaks.

    USAGE EXAMPLE:

    class Task(luigi.Task):
        password = PasswordParameter()
        ...
        def __init__(self, password):
            self.password = str(password)

    ...

    class SuperTask(luigi.Task):
        ...
        def requires(self):
            return [Task(password = PasswordValue('secret-password'))]
        ...
    """

    def __init__(self, description: str = None, positional: bool = True, always_in_help: bool = False):
        """
        You can specify three properties of the parameter in the constructor:
        :param description:    human readable description which goes to commandline help text.
        :param positional:     you must write the argument name if the value is false.
        :param always_in_help: whether the parameter description is in the commandline help whenever the
                               task is a subtask of the main task. It is very annoying if the flag is set!
        """

        # noinspection PyTypeChecker
        super().__init__(
            significant=False,                       # Password value has no effect on the outcome
            description=description,                 #
            positional=positional,                   #
            always_in_help=always_in_help,           #
            batch_method=None,                       # You cannot combine different objects into single object
            visibility=ParameterVisibility.PRIVATE   # The parameter  visible only inside the task
        )

    def serialize(self, parameter):
        return '***'


class PasswordValue:
    """
    Safe container for passwords. Use to hide password values from exceptions raised by luigi.Task
    """
    def __init__(self, value: str):
        self._value = value

    def __str__(self):
        return self._value
