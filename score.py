def calculate_tiling_card_score(card_number, tiling_stats, stat_percentiles, game_cards):
    """
    Calculates a score for a tiling given a card number based on the percentile
    rank of its stats.
    """
    card = game_cards[card_number-1]
    card_key, card_type = card["key"], card["type"]

    # If the tiling isn't relevant for the card
    if card_key == "" or card_type == "":
        return 100.0
    
    stat_value = tiling_stats[card_key]
    score = stat_percentiles[card_key][stat_value]

    if card_type == "max":
        return score
    
    if card_type == "min":
        return 100.0 - score