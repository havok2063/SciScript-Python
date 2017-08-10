
.. _sciserver-installation:

Installation
============

**Painless Installation**::

    pip install sdss-sciserver

**or to upgrade an existing SciServer installation**::

    pip install --upgrade sdss-sciserver

.. admonition:: Hint
    :class: hint

    By default, ``pip`` will update any underlying package on which sciserver depends. If you want to prevent that you can upgrade sciserver with ``pip install -U --no-deps sdss-sciserver``. This could, however, make sciserver to not work correctly. Instead, you can try ``pip install -U --upgrade-strategy only-if-needed sdss-sciserver``, which will upgrade a dependency only if needed.
