from flowy.scanner import attach


def reg(category, a=None, b=None, c=None):
    def wrapper(f):
        attach(lambda f_name, obj: f_name, category, f, a, b, c=c)
        return f
    return wrapper


@reg('cat_a', 1, 2, 3)
def test1(a, b, c, d, e, f_name):
    return (a, b, c, d, e, f_name)


@reg('cat_a', 10, 20, 30)
def test2(a, b, c, d, e, f_name):
    return (a, b, c, d, e, f_name)


@reg('cat_b', 100, 200, 300)
def test3(a, b, c, d, e, f_name):
    return (a, b, c, d, e, f_name)
