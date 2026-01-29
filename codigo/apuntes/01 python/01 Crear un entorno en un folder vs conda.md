


Creates a new isolated Python environment named `py_env` in the current folder using the built-in `venv` module.
```
python3 -m venv py_env
```
- python3: Specifies the Python interpreter to use. In this case, it uses Python 3.
- -m venv: Tells Python to run the `venv` module, which is responsible for creating virtual environments.
- m: Specifies that a module is to be run as a script.
- venv: The name of the module that creates virtual environments.
- py_env: The name of the directory where the virtual environment will be created. You can choose any name you prefer.

to activate the environment, use the following command:
```
py_env/bin/activate   
```