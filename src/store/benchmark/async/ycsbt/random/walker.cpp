#include "./walker.h"
#include <cassert>


namespace ycsbt {

static void
walker_construct(
    const double *E,  // probabilities
    double *B,  // temporary space
    double *F,  // cutoff  values
    uint32_t *IA,  // aliases
    uint32_t N) {  // size of prob. space

    double err = 1e-10;

    // init IA, F, B
    for (uint32_t i = 0; i < N; ++i) {
        IA[i] = i;
        F[i] = 0.0;
        B[i] = E[i] - 1.0 / N;
    }

    // find largest + and - diff and their positions in B
    for (uint32_t i = 0; i < N; ++i) {
        // test if the sum of diff has become significant
        double sum = 0.0;
        for (uint32_t m = 0; m < N; ++m) {
            sum += fabs(B[m]);
        }
        if (sum < err) {
            break;
        }

        double C = 0.0;
        double D = 0.0;
        uint32_t k, l;
        for (uint32_t j = 0; j < N; ++j) {
            if (B[j] < C) {
                C = B[j];
                k = j;
            } else if (B[j] > D) {
                D = B[j];
                l = j;
            }
        }

#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
        IA[k] = l;
        F[k] = 1.0 + C * N;
        B[k] = 0.0;
        B[l] = C + D;
#pragma GCC diagnostic warning "-Wmaybe-uninitialized"
    }
}

Walker::Walker(const std::vector<double> &prob):
    m_N((uint32_t) prob.size()),
    m_IA(m_N),
    m_F(m_N),
    m_unif_a(0, m_N - 1),
    m_unif_b(0.0, 1.0) {
    assert(abs(std::accumulate(prob.begin(), prob.end(),
                               static_cast<double>(0.0) - 1.0) <= 1e-10));
    std::vector<double> B(m_N);
    walker_construct(prob.data(), B.data(), m_F.data(), m_IA.data(), m_N);
}

}   // namespace random
