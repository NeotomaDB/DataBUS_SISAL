SELECT DISTINCT
        site.*,
        notes.*,
        entity.*,
        entity_link_reference.*,
        reference.*,
        depths.*,
        dating.*,
        dating_lamina.*,
        sample.*,
        hiatus.*,
        ba_ca.*,
        mg_ca.*,
        p_ca.*,
        sr_ca.*,
        u_ca.*,
        sr_isotopes.*,
        d18o.*,
        d13c.*,
        original_chronology.*,
        sisal_chronology.*,
        compe.*,
        composite_link_entity.*,
        single_ent.*
    FROM
        site
    LEFT JOIN notes USING (site_id)
    LEFT JOIN entity USING (site_id)
    LEFT JOIN entity_link_reference ON entity.entity_id = entity_link_reference.entity_id
    LEFT JOIN reference USING (ref_id)
    LEFT JOIN (
        SELECT depth_dating AS depth, entity_id FROM dating
        UNION DISTINCT
        SELECT depth_lam AS depth, entity_id FROM dating_lamina
        UNION DISTINCT
        SELECT depth_sample AS depth, entity_id FROM sample
    ) AS depths ON depths.entity_id = entity.entity_id
    LEFT JOIN dating ON dating.entity_id = entity.entity_id AND dating.depth_dating = depths.depth
    LEFT JOIN dating_lamina ON dating_lamina.entity_id = entity.entity_id AND dating_lamina.depth_lam = depths.depth
    LEFT JOIN sample ON entity.entity_id = sample.entity_id AND sample.depth_sample = depths.depth
    LEFT JOIN hiatus USING (sample_id)
    LEFT JOIN ba_ca USING (sample_id)
    LEFT JOIN mg_ca USING (sample_id)
    LEFT JOIN p_ca USING (sample_id)
    LEFT JOIN sr_ca USING (sample_id)
    LEFT JOIN u_ca USING (sample_id)
    LEFT JOIN sr_isotopes USING (sample_id)
    LEFT JOIN d18o USING (sample_id)
    LEFT JOIN d13c USING (sample_id)
    LEFT JOIN original_chronology USING (sample_id)
    LEFT JOIN sisal_chronology USING (sample_id)
    LEFT JOIN entity compe ON entity.entity_id = compe.entity_id
    LEFT JOIN composite_link_entity ON compe.entity_id = composite_entity_id
    LEFT JOIN entity single_ent ON composite_link_entity.single_entity_id = single_ent.entity_id
    WHERE site.site_id = %(site_id)s
    ORDER BY entity.entity_id, depths.depth;