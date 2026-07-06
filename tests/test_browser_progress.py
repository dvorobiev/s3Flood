from s3flood.browser import ProgressState, make_bar, render_progress_lines


class TestMakeBar:
    def test_empty_full_and_clamp(self):
        assert make_bar(0.0, 10) == "░" * 10
        assert make_bar(1.0, 10) == "█" * 10
        assert make_bar(1.7, 10) == "█" * 10
        assert make_bar(-0.5, 10) == "░" * 10

    def test_partial_length(self):
        bar = make_bar(0.5, 10)
        assert len(bar) == 10
        assert bar.count("█") == 5


class TestRenderProgressLines:
    def test_shows_current_file_and_counts(self):
        p = ProgressState(title="Копирование ↑", current="a.bin",
                          done=7, total=11, bytes_done=100, bytes_total=400)
        text = "".join(t for _s, t in render_progress_lines(p, width=50))
        assert "a.bin" in text
        assert "7/11" in text
        assert "Файлы" in text and "Объём" in text
        assert "Esc" in text

    def test_bytes_bar_hidden_when_total_zero(self):
        p = ProgressState(title="Удаление", current="a.bin", done=1, total=3)
        text = "".join(t for _s, t in render_progress_lines(p, width=50))
        assert "Объём" not in text

    def test_errors_shown(self):
        p = ProgressState(title="Копирование ↑", done=2, total=3, errors=2)
        errors_line = [t for _s, t in render_progress_lines(p, width=50)
                       if "Ошибок" in t][0]
        assert "Ошибок: 2" in errors_line

    def test_lines_fit_width(self):
        p = ProgressState(title="Копирование ↑", current="x" * 200,
                          done=1, total=2, bytes_done=1, bytes_total=2)
        for _s, t in render_progress_lines(p, width=30):
            assert len(t.rstrip("\n")) <= 30
