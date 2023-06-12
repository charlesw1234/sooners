from .settings import the_settings

def N_(msg: str): return msg
def _(msg: str):
    if the_settings.translations is None: return msg
    else: return the_settings.translations.gettext(msg)
