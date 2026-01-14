# Maximum Likelihood Optimization

from math import sqrt
import numpy as np
import numpy.fft as fft

tiny = np.finfo(float).tiny
eps = np.finfo(float).eps
sqeps = sqrt(eps)

class max_likelihood:
    def __init__(self, measurements, iv):
        self.m = measurements
        self.iv = iv
        self.iv_valid = False
        self.nfval = 0
        self.ngrad = 0

    def fval(self, v):
        self.nfval += 1
        f = .0
        px = v.p.shape[0]
        py = v.p.shape[1]
        for i in range(len(self.m)):
            (x, y) = (self.m[i].x, self.m[i].y)
            self.iv[i] = fft.fft2(v.p * v.o[x:x+px, y:y+py])
            for ix in range(px):
                for iy in range(py):
                    f += (abs(self.iv[i][ix, iy]) - self.m[i].p[ix, iy]) ** 2
        self.iv_valid = True
        return 2. * f

    def deriv_numeric(self, v, d):
        global sqeps
        a = self.fval(v + d * sqeps)
        b = self.fval(v - d * sqeps)
        return (a - b) / (2. * sqeps)

    def grad_numeric(self, v):
        e = v.zeros_like()
        g = v.zeros_like()
        dim = e.p.shape
        for i in range(dim[0]):
            for j in range(dim[1]):
                e.p[i, j] = 1.
                g.p[i, j] = self.deriv_numeric(v, e)
                e.p[i, j] = 0.
        dim = e.o.shape
        for i in range(dim[0]):
            for j in range(dim[1]):
                e.o[i, j] = 1.
                g.o[i, j] = self.deriv_numeric(v, e)
                e.o[i, j] = 0.
        return g

    def grad(self, v):
        assert(self.iv_valid)
        self.iv_valid = False
        self.ngrad += 1
        px = v.p.shape[0]
        py = v.p.shape[1]
        t = v.zeros_like()
        for i in range(len(self.m)):
            self.iv[i] = fft.ifft2((-self.m[i].p / abs(self.iv[i]) + 1.) * self.iv[i] * 2.)
            (x, y) = (self.m[i].x, self.m[i].y)
            t.o[x:x+px, y:y+py] += v.p.conjugate() * self.iv[i]
            t.p += v.o[x:x+px, y:y+py].conjugate() * self.iv[i]
        return t

class line_search:
    def __init__(self):
        pass

    def cond_armijo(self, y0, d0, y1, x1):
        return y0 + x1 * 1e-4 * d0 > y1

    def cond_wolfe(self, d0, d1):
        return d0 * .8 <= d1

    def cond_strong_wolfe(self, d0, d1):
        return abs(d0 * .8) >= abs(d1)

    def quadratic_fit_pdp(self, y0, d0, y1, x1):
        assert(d0 < .0 and x1 > .0)
        t0 = -d0 * x1
        t1 = 2. * (y1 - y0 + t0)
        if t1 == .0:
            return .0
        return t0 * x1 / t1

    def cubic_fit_pdpd(self, y0, d0, y1, x1, d1):
        assert(x1 > .0)
        d = (2. * (y0 - y1) + x1 * (d0 + d1)) / (x1 * x1)
        e = 1./ x1
        c = .5 * (d1 - d0) * e - 1.5 * d
        d *= e
        z = c * c - 3. * d * d0
        assert(z > .0)
        z = sqrt(z)
        d = 1. / (3. * d)
        x_1 = (-c - z) * d
        x_2 = (-c + z) * d
        if x_1 <= x1:
            return x_2
        if x_2 <= x1:
            return x_1
        if x_1 <= x_2:
            return x_1
        return x_2

    def cubic_fit_pdpp(self, y0, d0, y1, x1, y2, x2):
        assert(d0 < .0 and x1 > .0 and x2 > x1)
        c = (y1 - y0 - d0 * x1) / (x1 * x1)
        x2x2 = x2 * x2
        d = (y2 - y0 - d0 * x2 - c * x2x2) / (x2x2 * (x2 - x1))
        assert(d != .0)
        c = c - d * x1
        z = c * c - 3. * d * d0
        assert(z > .0)
        z = sqrt(z)
        d = 1. / (3. * d)
        x_1 = (-c - z) * d
        x_2 = (-c + z) * d
        if x_1 <= .0:
            return x_2
        if x_2 <= .0:
            return x_1
        if x_1 <= x_2:
            return x_1
        return x_2

    def cubic_fit_ppdp(self, y0, y1, x1, d1, y2, x2):
        assert(x1 > .0 and x2 > x1)
        x0 = -x1
        x2 -= x1
        c = (y0 - y1 - d1 * x0) / (x0 * x0)
        x2x2 = x2 * x2
        d = ((y2 - y1) / x2 - d1 - x2 * c) / (x2x2 - x0 * x2)
        c = c - d * x0
        z = c * c - 3. * d * d1
        assert(z > .0)
        z = sqrt(z)
        d = 1. / (3. * d)
        x_1 = (-c - z) * d
        x_2 = (-c + z) * d
        x = x_2
        if x_1 <= .0:
            pass
        elif x_2 <= .0:
            x = x_1
        elif x_1 < x_2:
            x = x_1
        return x + x1

    def derivative(self, ep, dv, y0, delta):
        t = ep + delta * dv


class line_search_wolfe(line_search):
    def __init__(self, mlh):
        self.mlh = mlh
        self.step = 1.
        self.old_d = .0
        self.n = 0

    def search(self, y0, sv, dv, gv):
        global sqeps, tiny
        print('%d: search' % self.n)
        self.n += 1
        d0 = dv * gv
        if d0 >= .0:
            return ('restart', y0, sv, gv)
        if self.old_d == .0:
            alpha = .1
        else:
            alpha = self.step * self.old_d / d0
        self.old_d = d0
        state = 'init'
        x0 = .0
        (y1, x1, d1) = (.0, .0, .0)
        (y2, x2, d2) = (.0, .0, .0)
        (y3, x3, d3) = (.0, .0, .0)
        print ("state y0 d0, y1 x1 d1, y2 x2 d2")
        while state != 'stop':
            print("%s %e %e, %e %e %e, %e %e %e" % (state, y0, d0,  y1, x1, d1,  y2, x2, d2))
            if state == 'init':
                x1 = alpha
                ep = sv + x1 * dv
                y1 = self.mlh.fval(ep)
                if not self.cond_armijo(y0, d0, y1, x1):
                    state = 'pdp'
                else:
                    gv = self.mlh.grad(ep)
                    d1 = dv * gv
                    if self.cond_wolfe(d0, d1):
                        self.step = x1
                        return ('next', y1, ep, gv)
                    state = 'pdpd'
            elif state == 'pdp':
                x2 = self.quadratic_fit_pdp(y0, d0, y1, x1)
                if x2 <= .0:
                    (y1, x1, d1, y2, x2, d2) = (y0, x0, d0, y1, x1, d1)
                    state = 'bisect'
                else:
                    ep = sv + x2 * dv
                    y2 = self.mlh.fval(ep)
                    if not self.cond_armijo(y0, d0, y2, x2):
                        if x1 > x2:
                            (x1, y1, d1, x2, y2, d2) = (x2, y2, d2, x1, y1, d1)
                        if x1 == x2:
                            (y1, x1, d1, y2, x2, d2) = (y0, x0, d0, y1, x1, d1)
                            state = 'bisect'
                        else:
                            state = 'pdpp'
                    else:
                        gv = self.mlh.grad(ep)
                        d2 = dv * gv
                        if self.cond_wolfe(d0, d2):
                            self.step = x1
                            return ('next', y2, ep, gv)
                        (y1, x1, d1) = (y2, x2, d2)
                        (y2, x2, d2) = (.0, .0, .0)
                        state = 'pdpd'
            elif state == 'pdpd':
                if (d1 < d0):
                    state = 'step'
                else:
                    x2 = self.cubic_fit_pdpd(y0, d0, y1, x1, d1)
                    if x2 <= x1:
                        state = 'step'
                    else:
                        ep = sv + x2 * dv
                        y2 = self.mlh.fval(ep)
                        if not self.cond_armijo(y0, d0, y2, x2):
                            state = 'pdpp'
                        else:
                            gv = self.mlh.grad(ep)
                            d2 = dv * gv
                            if self.cond_wolfe(d0, d2):
                                self.step = x1
                                return ('next', y2, ep, gv)
                            (x1, y1, d1) = (x2, y2, d2)
                            (x2, y2, d2) = (.0, .0, .0)
                            state = 'step'
            elif state == 'pdpp':
                x3 = self.cubic_fit_pdpp(y0, d0, y1, x1, y2, x2)
                if x3 < sqeps or x3 >= x2 or abs(x1-x3) < sqeps:
                    (y1, x1, d1, y2, x2, d2) = (y0, x0, d0, y1, x1, d1)
                    state = 'bisect'
                else:
                    ep = sv + x3 * dv
                    y3 = self.mlh.fval(ep)
                    if not self.cond_armijo(y0, d0, y3, x3):
                        if x3 < x1:
                            (y2, x2, d2) = (y1, x1, d1)
                            (y1, x1, d1) = (y3, x3, d3)
                            (y3, x3, d3) = (.0, .0, .0)
                        else:
                            (y1, x1, d1, y2, x2, d2) = (y0, x0, d0, y1, x1, d1)
                            state = 'bisect'
                    else:
                        gv = self.mlh.grad(ep)
                        d3 = dv * gv
                        if self.cond_wolfe(d0, d3):
                            self.step = x1
                            return ('next', y3, ep, gv)
                        (y2, x2, d2) = (y1, x1, d1)
                        (y1, x1, d1) = (y3, x3, d3)
                        (y3, x3, d3) = (.0, .0, .0)
                        state = 'ppdp'
            elif state == 'ppdp':
                x3 = self.cubic_fit_ppdp(y0, y1, x1, d1, y2, x2)
                if x3 < sqeps or x3 >= x2 or abs(x1-x3) < sqeps:
                    state = 'bisect'
                else:
                    ep = sv + x3 * dv
                    y3 = self.mlh.fval(ep)
                    if not self.cond_armijo(y0, d0, y3, x3):
                        (y2, x2, d2) = (y3, x3, d3)
                        (y3, x3, d3) = (.0, .0, .0)
                        state = 'ppdp'
                    else:
                        gv = self.mlh.grad(ep)
                        d3 = dv * gv
                        if self.cond_wolfe(d0, d3):
                            self.step = x1
                            return ('next', y3, ep, gv)
                        (y1, x1, d1) = (y3, x3, d3)
                        (y3, x3, d3) = (.0, .0, .0)
            elif state == 'step':
                x2 = 3. * x1
                ep = sv + x2 * dv
                y2 = self.mlh.fval(ep)
                if not self.cond_armijo(y0, d0, y2, x2):
                    state = 'ppdp'
                else:
                    (y1, x1, d1) = (y2, x2, d2)
                    (y2, x2, d2) = (.0, .0, .0)
                    gv = self.mlh.grad(ep)
                    d1 = dv * gv
                    if self.cond_wolfe(d0, d1):
                        self.step = x1
                        return ('next', y1, ep, gv)
            elif state == 'bisect':
                assert(x1 < x2)
                assert(not self.cond_armijo(y0, d0, y2, x2))
                if not self.cond_armijo(y0, d0, y1, x1):
                    (y1, x1, d1) = (y0, .0, d0)
                #if x2 - x1 < sqeps:
                    #print "1-fval(sv) =", self.mlh.fval(sv)
                    #self.display()
                    #return ('restart', y1, ep, gv)
                x3 = .5 * (x1 + x2)
                ep = sv + x3 * dv
                y3 = self.mlh.fval(ep)
                if not self.cond_armijo(y0, d0, y3, x3):
                    (y2, x2, d2) = (y3, x3, d3)
                    (y3, x3, d3) = (.0, .0, .0)
                else:
                    (y1, x1, d1) = (y3, x3, d3)
                    (y3, x3, d3) = (.0, .0, .0)
                    gv = self.mlh.grad(ep)
                    d1 = dv * gv
                    if self.cond_wolfe(d0, d1):
                        self.step = x1
                        return ('next', y1, ep, gv)
                    if abs(x1 - x2) < sqeps:
                        print("!restart: x1 ~ x2")
                        #self.display()
                        return ('restart', y1, ep, gv)
            else:
                assert(False)

class line_search_bisect(line_search):
    def __init__(self, mlh):
        self.mlh = mlh
        self.step = 1.
        self.old_d = .0
        self.n = 0

    def result(self, state, y, x, sv, gv):
        if state == 'next':
            self.step = x
        return (state, y, sv, gv)

    def search(self, y0, sv, dv, gv):
        print('%d: search' % self.n)
        self.n += 1
        d0 = dv * gv
        if d0 >= .0:
            return self.result('restart', y0, .0, sv, gv)
        if self.old_d == .0:
            alpha = .1
        else:
            alpha = self.step * self.old_d / d0
        self.old_d = d0
        state = 'init'
        x0 = .0
        (y1, x1, d1) = (.0, .0, .0)
        (y2, x2, d2) = (.0, .0, .0)
        (y3, x3, d3) = (.0, .0, .0)
        print ("state y0 d0, y1 x1 d1, y2 x2 d2")
        while state != 'stop':
            print("%s %e %e, %e %e %e, %e %e %e" % (state, y0, d0,  y1, x1, d1,  y2, x2, d2))
            if state == 'init':
                x1 = alpha
                ep = sv + x1 * dv
                y1 = self.mlh.fval(ep)
                if self.cond_armijo(y0, d0, y1, x1):
                    gv = self.mlh.grad(ep)
                    d1 = dv * gv
                    if self.cond_wolfe(d0, d1):
                        return self.result('next', y1, x1, ep, gv)
                    if d1 < .0:
                        state = 'expand'
                (y2, x2, d2) = (y1, x1, d1)
                (y1, x1, d1) = (.0, .0, .0)
                state = 'shrink'
            elif state == 'expand':     # expand x1 beyond armijo line
                x2 = 3. * x1
                ep = sv + x2 * dv
                y2 = self.mlh.fval(ep)
                if self.cond_armijo(y0, d0, y2, x2):
                    gv = self.mlh.grad(ep)
                    d2 = dv * gv
                    if self.cond_wolfe(d0, d2):
                        return self.result('next', y2, x2, ep, gv)
                    if d2 < .0:
                        (y1, x1, d1) = (y2, x2, d2)
                        (y2, x2, d2) = (.0, .0, .0)
                        continue
                state = 'shrink'
            elif state == 'shrink':     # shrink bracket ]x1, x2[
                x3 = (x1 + x2) / 2.
                ep = sv + x3 * dv
                y3 = self.mlh.fval(ep)
                if (x3 - x1 < sqeps) or (x2 - x3 < sqeps):
                    gv = self.mlh.grad(ep)
                    d3 = dv * gv
                    return self.result('restart', y3, x3, ep, gv)
                if self.cond_armijo(y0, d0, y3, x3):
                    gv = self.mlh.grad(ep)
                    d3 = dv * gv
                    if self.cond_wolfe(d0, d3):
                        return self.result('next', y3, x3, ep, gv)
                    if d3 < .0:
                        (y1, x1, d1) = (y3, x3, d3)
                        (y3, x3, d3) = (.0, .0, .0)
                        continue
                (y2, x2, d2) = (y3, x3, d3)
                (y3, x3, d3) = (.0, .0, .0)
            else:
                assert(False)

class conjugate_gradient_prp:
    def __init__(self, mlh, ls):
        self.mlh = mlh
        self.ls = ls
        self.state = 'init'
        self.restarts = 0
        self.y0 = .0
        self.n = 0

    def update_dv(self):
        dprod = self.gv * self.gvp
        ngv = self.gv * self.gv
        if abs(dprod) >= .8 * ngv:
            dv = -self.gv
            print("!powell: steepest descent (|%e| >= .8 * %e)" % (dprod, ngv))
        else:
            beta = (ngv - dprod) / self.ngv
            if beta <= .0:
                dv = -self.gv
                print("!negative beta: steepest descent")
            else:
                dv = -self.gv + beta * self.dv
        self.ngv = ngv
        self.dv = dv
        return dv

    def step(self, sv):
        print("%d: state %s" % (self.n, self.state))
        self.n += 1
        if self.state == 'init':
            self.y0 = self.mlh.fval(sv)
            print('max likelihood function value = %e' % self.y0)
            gv = self.mlh.grad(sv)
            dv = -gv
            self.gv = gv
            self.ngv = gv * gv
            self.dv = dv
            (state, self.y0, svn, gvn) = self.ls.search(self.y0, sv, dv, gv)
        elif self.state == 'restart':
            self.restarts += 1
            dv = -self.gv
            self.dv = dv
            self.ngv = self.gv * self.gv
            (state, self.y0, svn, gvn) = self.ls.search(self.y0, sv, dv, self.gv)
        elif self.state == 'next':
            self.restarts = 0
            dv = self.update_dv()
            (state, self.y0, svn, gvn) = self.ls.search(self.y0, sv, dv, self.gv)
        else:
            assert(False)
        sv.assign(svn)
        print("y=%e" % self.y0)
        self.state = state
        self.gvp = self.gv
        self.gv = gvn
        if sqrt(abs(gvn)) <= sqeps:
            print('!stop (abs(gv)=%e' % t)
            return (True, sv)
        if (self.state == 'restart') and (self.restarts >= 2):
            print('!stop (> 2 consecutive restarts)')
            return (True, sv)
        return (False, sv)
