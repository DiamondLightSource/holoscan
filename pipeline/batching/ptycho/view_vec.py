# Vector of views

class view_vec:
    def __init__(self, l):
        self.l = l

    def __getitem__(self, i):
        return self.l[i]

    def __setitem__(self, i, x):
        self.l[i] = x

    def __len__(self):
        return len(self.l)

    def __add__(self, o):
        r = []
        for i in range(len(self.l)):
            r.append(self.l[i] + o[i])
        return view_vec(r)

    def __sub__(self, o):
        r = []
        for i in range(len(self.l)):
            r.append(self.l[i] - o[i])
        return view_vec(r)

    def __mul__(self, f):
        r = []
        for i in range(len(self.l)):
            r.append(self.l[i] * f)
        return view_vec(r)

    def __rmul__(self, f):
        return self.__mul__(f)
