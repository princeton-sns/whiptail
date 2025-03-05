#ifndef WORKLOAD_RANDOM_ZIPFIAN_DISTRIBUTION_H_
#define WORKLOAD_RANDOM_ZIPFIAN_DISTRIBUTION_H_


#include <random>
#include <cmath>
#include <iostream>
#include <bits/uniform_int_dist.h>

namespace ycsbt {
//amespace random {

/*!
 * Zipfian Distribution is to generate a Zipfian Distribution referring to
 * https://github.com/brianfrankcooper/YCSB/blob/master/core/src/main/java/site/ycsb/generator/ZipfianGenerator.java
 *
 * Its probability distribution is defined as:
 * $P(X = r) = \frac{c}{r^s}$
 *      where r represents ranking
 *      c represents the nomorlize constant that needs to be calculated
 *          c = 1 / zeta, function calculate_zeta calculates zeta.
 *      s represets zipfian constant, usually around one
 *          ZIPFIAN_CONSTANT = 0.99, here
 */
template<typename IntType = uint64_t>
class zipfian_distribution{
public:
    // a Default Constructor
    zipfian_distribution() : m_items(0), m_base(0), m_zipfianconstant(0.99), m_zetan(1.0), m_zeta2(1.0), m_eta(1.0) {
        m_dist = std::uniform_real_distribution<double>(0.0, 1.0);
    }
    // Constructor
    zipfian_distribution(IntType min, IntType max, double zipfianconstant){
        m_items = max - min + 1;
        m_base = min;
        m_zipfianconstant = zipfianconstant;

        m_zetan = calculate_zeta(m_items, m_zipfianconstant);
        m_zeta2 = calculate_zeta(2,zipfianconstant);
        m_eta = (1 - std::pow(2.0 / m_items, 1 - m_zipfianconstant)) / (1 - m_zeta2 / m_zetan);
        // init the uniform_real_distribution
        m_dist = std::uniform_real_distribution<double> (0.0,1.0);

    }

    // function
    template<class RNG> IntType operator()(RNG &rng){
        double u = m_dist(rng);
        double uz = u * m_zetan;

        if (uz < 1.0) {return m_base;}
        if (uz < 1.0 + std::pow(0.5, m_zipfianconstant)){return m_base + 1;}

        IntType ret = m_base + (IntType) ((m_items) * \
                std::pow(m_eta * u - m_eta + 1 , 1.0 / (1.0 - m_zipfianconstant)));
        return ret;

    }

private:
    IntType m_items;
    IntType m_base;
    double m_zipfianconstant;
    double m_zetan;
    double m_zeta2;
    double m_eta;
    std::uniform_real_distribution<double> m_dist;

    static double calculate_zeta(IntType n, double zipfianconstant){
        double sum = 0;
        for (IntType i = 1; i <= n; i++){
            sum +=  1 / (std::pow(i , zipfianconstant));
        }
        return sum;
    }
};



//}   // namespace random
}   // namespace workload

#endif
