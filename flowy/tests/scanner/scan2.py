from flowy.scanner import attach


def reg(f):
    attach(lambda f_name, obj: f_name, None, f)
    return f


@reg
def test4(f_name):
    pass

@reg
def test5(f_name):
    pass
