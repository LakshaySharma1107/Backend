CREATE OR REPLACE PROCEDURE identify_contact(
    IN input_email VARCHAR(255),
    IN input_phoneNumber VARCHAR(255),
    OUT result_json JSON
)
LANGUAGE plpgsql AS $$
DECLARE
    match_count INT;
    primary_id INT;
    new_contact_id INT;
    has_new_info BOOLEAN;
    primary_email VARCHAR(255);
    primary_phone VARCHAR(255);
BEGIN
    -- Handle edge case: both inputs are null
    IF input_email IS NULL AND input_phoneNumber IS NULL THEN
        RAISE EXCEPTION 'At least one of email or phoneNumber must be provided';
    END IF;

    -- Step 1: Find matching contacts
    SELECT COUNT(*) INTO match_count
    FROM Contact
    WHERE (email = input_email AND input_email IS NOT NULL)
       OR (phoneNumber = input_phoneNumber AND input_phoneNumber IS NOT NULL)
       AND deletedAt IS NULL;

    -- Step 2: No matches - create new primary contact
    IF match_count = 0 THEN
        INSERT INTO Contact (phoneNumber, email, linkPrecedence, createdAt, updatedAt)
        VALUES (input_phoneNumber, input_email, 'primary', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        RETURNING id INTO primary_id;

        -- Return new contact
        SELECT JSON_BUILD_OBJECT(
            'contact', JSON_BUILD_OBJECT(
                'primaryContatctId', primary_id,
                'emails', ARRAY[COALESCE(input_email, '')] FILTER (WHERE input_email IS NOT NULL),
                'phoneNumbers', ARRAY[COALESCE(input_phoneNumber, '')] FILTER (WHERE input_phoneNumber IS NOT NULL),
                'secondaryContactIds', ARRAY[]::INT[]
            )
        ) INTO result_json;
        RETURN;
    END IF;

    -- Step 3: Find primary contact (oldest by createdAt)
    SELECT id, email, phoneNumber INTO primary_id, primary_email, primary_phone
    FROM Contact
    WHERE (email = input_email AND input_email IS NOT NULL)
       OR (phoneNumber = input_phoneNumber AND input_phoneNumber IS NOT NULL)
       AND deletedAt IS NULL
    ORDER BY createdAt ASC
    LIMIT 1;

    -- Step 4: Check for new info
    SELECT NOT EXISTS (
        SELECT 1 FROM Contact
        WHERE (email = input_email OR input_email IS NULL)
          AND (phoneNumber = input_phoneNumber OR input_phoneNumber IS NULL)
          AND deletedAt IS NULL
    ) INTO has_new_info;

    -- Step 5: If new info, create secondary contact
    IF has_new_info THEN
        INSERT INTO Contact (phoneNumber, email, linkedId, linkPrecedence, createdAt, updatedAt)
        VALUES (input_phoneNumber, input_email, primary_id, 'secondary', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        RETURNING id INTO new_contact_id;
    END IF;

    -- Step 6: Update existing contacts to link to primary
    UPDATE Contact
    SET linkedId = primary_id,
        linkPrecedence = 'secondary',
        updatedAt = CURRENT_TIMESTAMP
    WHERE (email = input_email AND input_email IS NOT NULL)
       OR (phoneNumber = input_phoneNumber AND input_phoneNumber IS NOT NULL)
       AND id != primary_id
       AND linkPrecedence = 'primary'
       AND deletedAt IS NULL;

    -- Step 7: Consolidate and return result
    SELECT JSON_BUILD_OBJECT(
        'contact', JSON_BUILD_OBJECT(
            'primaryContatctId', primary_id,
            'emails', (
                SELECT ARRAY[primary_email] || (
                    SELECT ARRAY_AGG(DISTINCT email)
                    FROM Contact
                    WHERE (email = input_email AND input_email IS NOT NULL)
                       OR (phoneNumber = input_phoneNumber AND input_phoneNumber IS NOT NULL)
                       AND deletedAt IS NULL
                       AND email IS NOT NULL
                       AND email != primary_email
                )
            ),
            'phoneNumbers', (
                SELECT ARRAY[primary_phone] || (
                    SELECT ARRAY_AGG(DISTINCT phoneNumber)
                    FROM Contact
                    WHERE (email = input_email AND input_email IS NOT NULL)
                       OR (phoneNumber = input_phoneNumber AND input_phoneNumber IS NOT NULL)
                       AND deletedAt IS NULL
                       AND phoneNumber IS NOT NULL
                       AND phoneNumber != primary_phone
                )
            ),
            'secondaryContactIds', (
                SELECT ARRAY_AGG(id)
                FROM Contact
                WHERE (email = input_email AND input_email IS NOT NULL)
                   OR (phoneNumber = input_phoneNumber AND input_phoneNumber IS NOT NULL)
                   AND id != primary_id
                   AND linkPrecedence = 'secondary'
                   AND deletedAt IS NULL
                )
            )
        )
    ) INTO result_json;
END;
$$;