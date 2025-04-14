"""(c) 2024, Elastic Co.
Author: Adhish Thite <adhish.thite@elastic.co>
"""

from app.utils.helpers import generate_hash


def test_generate_hash():
    # Test with a simple dictionary
    test_data = {"key": "value", "number": 123}
    hash_result = generate_hash(test_data)

    # Test that the hash is a string
    assert isinstance(hash_result, str)

    # Test that the hash has the correct length (MD5 produces 32 character hashes)
    assert len(hash_result) == 32

    # Test that the same input produces the same hash
    assert generate_hash(test_data) == hash_result

    # Test that different inputs produce different hashes
    different_data = {"key": "different", "number": 456}
    assert generate_hash(different_data) != hash_result
