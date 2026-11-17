-- Optional, compatible upgrade of campaign diagnostics. No table or data is removed.
-- Apply AFTER 01_install_or_upgrade.sql, with the project administrator account.
CREATE OR REPLACE FUNCTION public.diag_campaign_info(p_campaign text,p_writer_token text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path = '' AS $$
DECLARE c public.diag_campaigns%ROWTYPE;
BEGIN
  SELECT * INTO c FROM public.diag_campaigns WHERE id=p_campaign;
  IF c.id IS NULL OR p_writer_token IS NULL OR length(p_writer_token)<32
      OR c.writer_token_hash <> encode(sha256(convert_to(p_writer_token,'UTF8')),'hex') THEN
    RAISE EXCEPTION 'INVALID_WRITER';
  END IF;
  RETURN jsonb_build_object(
    'active', c.active AND now()>=c.opens_at AND now()<=c.closes_at,
    'enabled', c.active, 'test_mode', c.test_mode, 'server_now', now(),
    'requires_access_code', c.access_code_hash IS NOT NULL,
    'opens_at', c.opens_at, 'closes_at', c.closes_at);
END;
$$;
REVOKE ALL ON FUNCTION public.diag_campaign_info(text,text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.diag_campaign_info(text,text) TO anon, authenticated;
