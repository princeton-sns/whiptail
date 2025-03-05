#ifndef WORKLOAD_RANDOM_TRUNCATED_ZIPFIAN_H_
#define WORKLOAD_RANDOM_TRUNCATED_ZIPFIAN_H_


#include <random>

#include "./walker.h"


namespace ycsbt {

/*!
 * A truncated zipfian distribution over [0, N - 1], where all items with
 * probability smaller than a threshold in the original zipfian distribution
 * are uniformly sampled with the same probability, while all others are
 * sampled with the same probabilities as in the zipfian.
 *
 * Note that this class does not satisfy RandomNumberDistribution requirement
 * due to missing many required interfaces.
 */
class TruncatedZipfian {
 public:
    using result_type = uint32_t;

    // using param_type = /* something */;

    //! Default constructs truncated zipfian in an invalid state.
    TruncatedZipfian():
        m_N(0) {}

    /*!
     * Constructs a truncated zipfian(s) distribution over all the integers in
     * [0, N - 1], with all items with probabilities smaller than \p
     * prob_to_truncate in the zipfian are sampled with the same probabilty,
     * while the remaining are sampled with the probabilities in the zipfian.
     */
    TruncatedZipfian(uint32_t N, double s, double prob_to_truncate);

    template<typename RNG>
    inline uint32_t
    operator()(RNG &rng) {  // NOLINT(runtime/references)
        uint32_t x = m_walker(rng);
        if (x >= m_min_in_unif) {
            x = m_unif_rem(rng);
        }
        return x;
    }

    constexpr uint32_t
    GetMinTruncatedItem() const {
        return m_min_in_unif;
    }

    double GetProbability(uint32_t x) const;

    constexpr double
    GetTotalTruncatedProbability() const {
        return m_total_prob_trunc;
    }

 private:
    uint32_t    m_N;
    uint32_t    m_min_in_unif;
    double      m_s;
    double      m_H;
    double      m_total_prob_trunc;

    Walker      m_walker;
    std::uniform_int_distribution<uint32_t> m_unif_rem;
};

}   // namespace random

#endif      // WORKLOAD_RANDOM_TRUNCATED_ZIPFIAN_H_
