#include "./truncated_zipfian.h"

#include <vector>

#include "./walker.h"


namespace ycsbt {

TruncatedZipfian::TruncatedZipfian(uint32_t N, double s,
                                   double prob_to_truncate):
    m_N(N),
    m_s(s) {
    double neg_s = -s;

    // compute H_{n,s}
    double H = 0.0;
    std::vector<double> probs;
    probs.reserve(N);
    for (uint32_t i = 0; i < N; ++i) {
        double prob = exp(log(static_cast<double>(i + 1)) * neg_s);
        probs.push_back(prob);
        H += prob;
    }
    m_H = H;

    for (m_min_in_unif = 0; m_min_in_unif < N; ++m_min_in_unif) {
        probs[m_min_in_unif] /= H;
        if (probs[m_min_in_unif] < prob_to_truncate) {
            break;
        }
    }

    if (m_min_in_unif < m_N) {
        probs[m_min_in_unif] = 1.0;
        for (uint32_t i = 0; i < m_min_in_unif; ++i) {
            probs[m_min_in_unif] -= probs[i];
        }
        if (probs[m_min_in_unif] < 0)
            probs[m_min_in_unif] = 0.0;
        m_total_prob_trunc = probs[m_min_in_unif];
        probs.resize(m_min_in_unif + 1);
    }
    m_walker = Walker(probs);

    if (m_min_in_unif < m_N) {
        m_unif_rem.param(
            std::uniform_int_distribution<uint32_t>(m_min_in_unif,
                                                    m_N - 1).param());
    } else {
        // Not necessary here. But in case something goes wrong with the Walker
        // impl., we won't end up with an exception.
        m_unif_rem.param(
            std::uniform_int_distribution<uint32_t>(0, m_N-1).param());
    }
}

double
TruncatedZipfian::GetProbability(uint32_t x) const {
    if (x < m_min_in_unif) {
        return exp(-m_s * log(1 + x)) / m_H;
    }
    if (x < m_N) {
        return m_total_prob_trunc / (m_N - m_min_in_unif);
    }
    return 0.0;
}

}   // namespace random
