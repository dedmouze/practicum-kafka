from message_filter.censorship import censor_text


def test_masks_banned_words_case_insensitively() -> None:
    assert censor_text("Bad and VERY bad", ["bad"]) == "*** and VERY ***"


def test_does_not_mask_part_of_another_word() -> None:
    assert censor_text("cat concatenate cat!", ["cat"]) == "*** concatenate ***!"


def test_prefers_longer_phrase() -> None:
    assert censor_text("very bad idea", ["bad", "very bad"]) == "******** idea"


def test_empty_dictionary_does_not_change_text() -> None:
    assert censor_text("ordinary message", []) == "ordinary message"
