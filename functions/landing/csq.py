"""Consequence class of one ALT allele from a SnpEff ANN string. Shared by the landing function (VAST DB arms) and the
file arm of the SFS query, so the three arms classify sites with the same rule.

SnpEff orders ANN entries by impact; the first entry for the allele is its most severe consequence. Combined terms
('missense_variant&splice_region_variant') count as missense. Anything else, or no entry for the allele, is 'other'.
"""


def classify(ann, alt):
    for entry in ann.split(","):
        f = entry.split("|")
        if len(f) > 1 and f[0] == alt:
            terms = f[1].split("&")
            if "missense_variant" in terms: return "missense"
            if "synonymous_variant" in terms: return "synonymous"
            return "other"
    return "other"
