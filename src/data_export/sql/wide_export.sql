SELECT DISTINCT
        st.*,
        nt.notes,
        en.entity_id, en.entity_name, en.entity_status,
		en.corresponding_current, en.persist_id, en.depth_ref,
		en.geology, en.rock_age, en.wokam, en.vegetation_type, en.land_use,
		en.copernicus_lcc, en.cover_type, en.cover_thickness,
		en.host_rock_trace_elements, en.drip_water_trace_elements, en.distance_entrance,
		en.speleothem_type, en.drip_type, en.drip_height, en.d13C, en.d18O,
		en.iso_std, en.d18O_water_equilibrium, en.d18O_dripwater_carbonate_difference,
		en.organics, en.fluid_inclusions, en.mineralogy_petrology_fabric,
		en.clumped_isotopes, en.noble_gas_temperatures, en.C14, en.ODL,
		en.Sr_Ca, en.Sr_Ca_method, en.Sr_Ca_std, en.Sr_Ca_downsampled, en.Sr_Ca_downsampling_method,
		en.Mg_Ca, en.Mg_Ca_method, en.Mg_Ca_std, en.Mg_Ca_downsampled, en.Mg_Ca_downsampling_method,
        en.Ba_Ca, en.Ba_Ca_method, en.Ba_Ca_std, en.Ba_Ca_downsampled, en.Ba_Ca_downsampling_method,
        en.U_Ca, en.U_Ca_method, en.U_Ca_std, en.U_Ca_downsampled, en.U_Ca_downsampling_method,
        en.P_Ca, en.P_Ca_method, en.P_Ca_std, en.P_Ca_downsampled, en.P_Ca_downsampling_method,
        en.Sr_isotopes, en.Sr_isotopes_method, en.Sr_isotopes_std,
        en.trace_elements_datafile, en.trace_elements_metadatafile, en.cave_map,
        en.contact, en.data_DOI_URL,
        elr.ref_id,
        ref.citation, ref.publication_DOI,
        depths.depth, depths.dating_id, depths.sample_id,
        dt.date_type, dt.dating_thickness, dt.lab_num, 
        dt.material_dated, dt.min_weight, dt.max_weight, dt.uncorr_age,
        dt.uncorr_age_uncert_pos, dt.uncorr_age_uncert_neg, dt.14C_correction,
        dt.calib_used, dt.date_used, dt.238U_content, dt.238U_uncertainty,
		dt.232Th_content, dt.232Th_uncertainty,
		dt.230Th_content, dt.230Th_uncertainty, 
		dt.230Th_232Th_ratio, dt.230Th_232Th_ratio_uncertainty, 
		dt.230Th_238U_activity, dt.230Th_238U_activity_uncertainty,
		dt.234U_238U_activity, dt.234U_238U_activity_uncertainty,
		dt.ini_230Th_232Th_ratio, dt.ini_230Th_232Th_ratio_uncertainty,
		dt.decay_constant, dt.corr_age,
		dt.corr_age_uncert_pos, dt.corr_age_uncert_neg, dt.date_used_lin_interp, dt.date_used_lin_reg, dt.date_used_Bchron,
		dt.date_used_Bacon, dt.date_used_OxCal, dt.date_used_copRa, dt.date_used_StalAge,
        dtl.depth_lam, dtl.lam_thickness, dtl.lam_age, dtl.lam_age_uncert_pos, dtl.lam_age_uncert_neg,
        smp.sample_thickness, smp.depth_sample, smp.mineralogy, smp.arag_corr,
        hia.hiatus,
        gap.gap,
        ba_ca.Ba_Ca_measurement, ba_ca.Ba_Ca_precision,
        mg_ca.Mg_Ca_measurement, mg_ca.Mg_Ca_precision,
        p_ca.P_Ca_measurement, p_ca.P_Ca_precision,
        sr_ca.Sr_Ca_measurement, sr_ca.Sr_Ca_precision,
        u_ca.U_Ca_measurement, u_ca.U_Ca_precision,
        sr_isotopes.Sr_isotopes_measurement, sr_isotopes.Sr_isotopes_precision,
        d18o.d18O_measurement, d18o.d18O_precision,
        d13c.d13C_measurement, d13c.d13C_precision,
        ochron.interp_age, ochron.interp_age_uncert_pos, ochron.interp_age_uncert_neg,
		ochron.age_model_type, ochron.ann_lam_check, ochron.dep_rate_check,
        sis_cr.lin_interp_age,
        sis_cr.lin_interp_age_uncert_pos,
        sis_cr.lin_interp_age_uncert_neg,
        sis_cr.lin_reg_age,
        sis_cr.lin_reg_age_uncert_pos,
        sis_cr.lin_reg_age_uncert_neg,
        sis_cr.Bchron_age,
        sis_cr.Bchron_age_uncert_pos,
        sis_cr.Bchron_age_uncert_neg,
        sis_cr.Bacon_age,
        sis_cr.Bacon_age_uncert_pos,
        sis_cr.Bacon_age_uncert_neg,
        sis_cr.OxCal_age,
        sis_cr.OxCal_age_uncert_pos,
        sis_cr.OxCal_age_uncert_neg,
        sis_cr.copRa_age,
        sis_cr.copRa_age_uncert_pos,
        sis_cr.copRa_age_uncert_neg,
        sis_cr.StalAge_age,
        sis_cr.StalAge_age_uncert_pos,
        sis_cr.StalAge_age_uncert_neg,
        complen.composite_entity_id
    FROM
        site AS st
    LEFT JOIN notes AS nt ON nt.site_id = st.site_id
    LEFT JOIN entity AS en ON en.site_id = st.site_id
    LEFT JOIN entity_link_reference AS elr ON en.entity_id = elr.entity_id
    LEFT JOIN sample AS smp ON smp.entity_id = en.entity_id
    LEFT JOIN original_chronology AS ochron ON ochron.sample_id = smp.sample_id
    LEFT JOIN reference AS ref ON ref.ref_id = elr.ref_id
    LEFT JOIN (
        SELECT depth_dating AS depth, dating_id, NULL as sample_id, NULL as dating_lamina_id, entity_id FROM dating
        UNION DISTINCT
        SELECT depth_lam AS depth, NULL as dating_id, NULL as sample_id, dating_lamina_id, entity_id FROM dating_lamina
        UNION DISTINCT
        SELECT depth_sample AS depth, NULL as dating_id, sample_id, NULL as dating_lamina_id, entity_id FROM sample
    ) AS depths ON depths.entity_id = en.entity_id AND smp.sample_id = depths.sample_id
    LEFT JOIN dating AS dt ON dt.entity_id = en.entity_id AND dt.depth_dating = depths.depth
    LEFT JOIN dating_lamina AS dtl ON dtl.entity_id = en.entity_id AND dtl.depth_lam = depths.depth
    LEFT JOIN hiatus AS hia ON hia.sample_id = smp.sample_id
    LEFT JOIN gap AS gap ON gap.sample_id = smp.sample_id
    LEFT JOIN ba_ca ON ba_ca.sample_id = smp.sample_id
    LEFT JOIN mg_ca ON mg_ca.sample_id = smp.sample_id
    LEFT JOIN p_ca ON p_ca.sample_id = smp.sample_id
    LEFT JOIN sr_ca ON sr_ca.sample_id = smp.sample_id
    LEFT JOIN u_ca ON u_ca.sample_id = smp.sample_id
    LEFT JOIN sr_isotopes ON sr_isotopes.sample_id = smp.sample_id
    LEFT JOIN d18o ON d18o.sample_id = smp.sample_id
    LEFT JOIN d13c ON d13c.sample_id = smp.sample_id
    LEFT JOIN sisal_chronology AS sis_cr ON sis_cr.sample_id = smp.sample_id
    LEFT JOIN entity AS compe ON en.entity_id = compe.entity_id
    LEFT JOIN composite_link_entity AS complen ON compe.entity_id = complen.composite_entity_id
    LEFT JOIN entity AS single_ent ON complen.single_entity_id = single_ent.entity_id
    WHERE en.entity_id = %(entity_id)s
    ORDER BY en.entity_id, depths.depth;