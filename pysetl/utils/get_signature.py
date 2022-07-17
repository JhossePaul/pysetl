from inspect import signature


def get_signature(function) -> list[str]:
    return list((
        x
        for x, y
        in signature(function).parameters.items()
    ))
