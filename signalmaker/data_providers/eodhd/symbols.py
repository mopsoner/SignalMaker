INITIAL_UNIVERSES = [
    ("ETF PEA", "PEA-oriented ETFs", "Europe", "ETF", "EUR"),
    ("ETF Europe UCITS", "European UCITS ETFs", "Europe", "ETF", "EUR"),
    ("Stocks Euronext Paris", "Large-cap Euronext Paris stocks", "France", "STOCK", "EUR"),
    ("Stocks Europe", "European stocks", "Europe", "STOCK", "EUR"),
    ("Benchmark Indices", "Benchmark index proxies", "Global", "INDEX", None),
    ("US Benchmarks", "US benchmark ETFs", "US", "ETF", "USD"),
]

INITIAL_ASSETS = [
    ("ETF PEA", "CW8.PA", "Amundi MSCI World", "ETF", True, True),
    ("ETF PEA", "EWLD.PA", "Amundi MSCI World", "ETF", True, True),
    ("ETF PEA", "PUST.PA", "Amundi Nasdaq 100", "ETF", True, True),
    ("ETF PEA", "ESE.PA", "BNP S&P 500", "ETF", True, True),
    ("ETF Europe UCITS", "SPY4.PA", "SPDR S&P 500", "ETF", False, True),
    ("ETF Europe UCITS", "R2US.PA", "Russell 2000 ETF", "ETF", False, True),
    ("ETF Europe UCITS", "OBLI.PA", "Bond ETF", "ETF", False, True),
    ("ETF PEA", "LQQ.PA", "Amundi Nasdaq 100 2x", "ETF", True, True),
    ("ETF PEA", "CL2.PA", "Amundi MSCI USA 2x", "ETF", True, True),
    ("ETF Europe UCITS", "PANX.PA", "Amundi PEA Nasdaq", "ETF", True, True),
    *[("Stocks Euronext Paris", s, n, "STOCK", None, None) for s, n in [
        ("AIR.PA", "Airbus"), ("TTE.PA", "TotalEnergies"), ("MC.PA", "LVMH"), ("OR.PA", "L'Oreal"),
        ("SAN.PA", "Sanofi"), ("BNP.PA", "BNP Paribas"), ("CS.PA", "AXA"), ("AI.PA", "Air Liquide"),
        ("CAP.PA", "Capgemini"), ("SU.PA", "Schneider Electric"), ("KER.PA", "Kering"),
        ("DG.PA", "Vinci"), ("RMS.PA", "Hermes"), ("EL.PA", "EssilorLuxottica"), ("RI.PA", "Pernod Ricard")]],
    *[("US Benchmarks", s, n, "ETF", False, False) for s, n in [("SPY.US", "SPDR S&P 500"), ("QQQ.US", "Invesco QQQ"), ("DIA.US", "SPDR Dow Jones"), ("IWM.US", "iShares Russell 2000")]],
]
